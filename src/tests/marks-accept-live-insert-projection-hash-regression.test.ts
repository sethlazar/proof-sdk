import { unlinkSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import express from 'express';
import * as Y from 'yjs';
import { HocuspocusProvider } from '@hocuspocus/provider';
import { prosemirrorToYXmlFragment } from 'y-prosemirror';
import { WebSocketServer } from 'ws';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (await predicate()) return;
    await sleep(25);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function mustJson<T>(res: Response, label: string): Promise<T> {
  const text = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${label}: HTTP ${res.status}: ${text.slice(0, 500)}`);
  return JSON.parse(text) as T;
}

function normalizeWsBase(collabWsUrl: string): string {
  const raw = collabWsUrl.replace(/\?slug=.*$/, '');
  try {
    const url = new URL(raw);
    if (url.hostname === 'localhost') url.hostname = '127.0.0.1';
    return url.toString();
  } catch {
    return raw.replace('ws://localhost:', 'ws://127.0.0.1:');
  }
}

const CLIENT_HEADERS = {
  'X-Proof-Client-Version': '0.31.2',
  'X-Proof-Client-Build': 'tests',
  'X-Proof-Client-Protocol': '3',
};

type CreateResponse = {
  slug: string;
  ownerSecret: string;
};

type CollabSessionResponse = {
  success: boolean;
  session: {
    collabWsUrl: string;
    slug: string;
    token: string;
    role: string;
  };
};

async function run(): Promise<void> {
  const dbName = `proof-marks-accept-live-insert-projection-hash-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  process.env.DATABASE_PATH = dbPath;
  process.env.COLLAB_EMBEDDED_WS = '1';

  const [{ apiRoutes }, { agentRoutes }, { setupWebSocket }, collab, milkdown] = await Promise.all([
    import('../../server/routes.js'),
    import('../../server/agent-routes.js'),
    import('../../server/ws.js'),
    import('../../server/collab.js'),
    import('../../server/milkdown-headless.js'),
  ]);

  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.use('/api', apiRoutes);
  app.use('/api/agent', agentRoutes);

  const server = createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });
  setupWebSocket(wss);

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  const address = server.address() as AddressInfo;
  const httpBase = `http://127.0.0.1:${address.port}`;

  await collab.startCollabRuntimeEmbedded(address.port);

  const createRes = await fetch(`${httpBase}/api/documents`, {
    method: 'POST',
    headers: { ...CLIENT_HEADERS, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      title: 'Live insert projection hash regression',
      markdown: 'Alpha beta gamma. delta\n',
      marks: {
        insert1: {
          kind: 'insert',
          by: 'human:test',
          createdAt: '2026-03-16T00:00:00.000Z',
          status: 'pending',
          content: ' delta',
          quote: 'delta',
          range: { from: 18, to: 24 },
          startRel: 'char:17',
          endRel: 'char:23',
        },
        'authored:human:test:18-24': {
          kind: 'authored',
          by: 'human:test',
          createdAt: '1970-01-01T00:00:00.000Z',
          quote: 'delta',
          range: { from: 18, to: 24 },
          startRel: 'char:17',
          endRel: 'char:23',
        },
      },
    }),
  });
  const created = await mustJson<CreateResponse>(createRes, 'create');

  const collabSessionRes = await fetch(`${httpBase}/api/documents/${created.slug}/collab-session`, {
    headers: { ...CLIENT_HEADERS, 'x-share-token': created.ownerSecret },
  });
  const collabSession = await mustJson<CollabSessionResponse>(collabSessionRes, 'collab-session');
  assert(collabSession.success === true, 'Expected successful collab session');

  const ydoc = new Y.Doc();
  const provider = new HocuspocusProvider({
    url: normalizeWsBase(collabSession.session.collabWsUrl),
    name: collabSession.session.slug,
    document: ydoc,
    parameters: {
      token: collabSession.session.token,
      role: collabSession.session.role,
    },
    token: collabSession.session.token,
    preserveConnection: false,
    broadcast: false,
  });

  let connected = false;
  let synced = false;
  provider.on('status', (event: { status: string }) => {
    if (event.status === 'connected') connected = true;
  });
  provider.on('synced', (event: { state?: boolean }) => {
    if (event.state !== false) synced = true;
  });

  try {
    await waitFor(() => connected && synced, 10_000, 'provider connected+synced');

    const parser = await milkdown.getHeadlessMilkdownParser();
    const parsed = milkdown.parseMarkdownWithHtmlFallback(
      parser,
      'Alpha beta gamma.<span data-proof="suggestion" data-id="insert1" data-by="human:test" data-kind="insert"><span data-proof="authored" data-by="human:test">delta</span></span>\n',
    );
    assert(Boolean(parsed.doc), 'Expected browser-style pending insert markdown to parse');

    ydoc.transact(() => {
      const fragment = ydoc.getXmlFragment('prosemirror');
      fragment.delete(0, fragment.length);
      prosemirrorToYXmlFragment(parsed.doc!, fragment);

      const markdownText = ydoc.getText('markdown');
      markdownText.delete(0, markdownText.length);
      markdownText.insert(0, 'Alpha beta gamma. delta\n');

      const marksMap = ydoc.getMap('marks');
      marksMap.clear();
      marksMap.set('insert1', {
        kind: 'insert',
        by: 'human:test',
        createdAt: '2026-03-16T00:00:00.000Z',
        status: 'pending',
        content: ' delta',
        quote: 'delta',
        range: { from: 18, to: 24 },
        startRel: 'char:17',
        endRel: 'char:23',
      });
      marksMap.set('authored:human:test:18-24', {
        kind: 'authored',
        by: 'human:test',
        createdAt: '1970-01-01T00:00:00.000Z',
        quote: 'delta',
        range: { from: 18, to: 24 },
        startRel: 'char:17',
        endRel: 'char:23',
      });
    }, 'test-browser-style-insert');

    await sleep(250);

    const acceptRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({ markId: 'insert1', by: 'human:test' }),
    });
    const acceptPayload = await mustJson<{
      success?: boolean;
      marks?: Record<string, unknown>;
      collab?: { status?: string; canonicalConfirmed?: boolean | null };
    }>(acceptRes, 'accept');

    assert(acceptPayload.success === true, 'Expected accept success for live browser-style insert');
    assert(
      acceptPayload.collab?.status === 'confirmed',
      `Expected confirmed collab status, got ${String(acceptPayload.collab?.status)}`,
    );
    assert(
      acceptPayload.collab?.canonicalConfirmed === true,
      `Expected canonicalConfirmed true, got ${String(acceptPayload.collab?.canonicalConfirmed)}`,
    );

    const stateRes = await fetch(`${httpBase}/api/agent/${created.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': created.ownerSecret },
    });
    const state = await mustJson<{ markdown?: string; marks?: Record<string, { status?: string }> }>(stateRes, 'state');
    assert(state.markdown === 'Alpha beta gamma. delta\n', `Expected accepted markdown, got ${JSON.stringify(state.markdown)}`);
    assert(!state.marks?.insert1, 'Expected pending insert mark to be removed after accept');

    console.log('✓ marks/accept accepts live browser-style inserts whose semantic space lives in mark content');
  } finally {
    try {
      provider.disconnect();
      provider.destroy();
    } catch {
      // ignore
    }
    ydoc.destroy();
    await collab.stopCollabRuntime();
    try {
      wss.close();
    } catch {
      // ignore
    }
    await new Promise<void>((resolve) => server.close(() => resolve()));
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        unlinkSync(`${dbPath}${suffix}`);
      } catch {
        // ignore
      }
    }
  }
}

run().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
