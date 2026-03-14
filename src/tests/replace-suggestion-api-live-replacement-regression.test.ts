import { unlinkSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import express from 'express';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

async function mustJson<T>(res: Response, label: string): Promise<T> {
  const text = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${label}: HTTP ${res.status}: ${text.slice(0, 500)}`);
  return JSON.parse(text) as T;
}

const CLIENT_HEADERS = {
  'X-Proof-Client-Version': '0.31.2',
  'X-Proof-Client-Build': 'tests',
  'X-Proof-Client-Protocol': '3',
};

async function run(): Promise<void> {
  const dbName = `proof-replace-suggestion-live-replacement-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  const previousDbPath = process.env.DATABASE_PATH;
  process.env.DATABASE_PATH = dbPath;
  process.env.COLLAB_EMBEDDED_WS = '1';

  const [{ apiRoutes }, { agentRoutes }, { setupWebSocket }, collab] = await Promise.all([
    import('../../server/routes.js'),
    import('../../server/agent-routes.js'),
    import('../../server/ws.js'),
    import('../../server/collab.js'),
  ]);

  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.use('/api', apiRoutes);
  app.use('/api/agent', agentRoutes);

  const server = createServer(app);
  const { WebSocketServer } = await import('ws');
  const wss = new WebSocketServer({ server, path: '/ws' });
  setupWebSocket(wss);

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  const address = server.address() as AddressInfo;
  const httpBase = `http://127.0.0.1:${address.port}`;

  await collab.startCollabRuntimeEmbedded(address.port);

  try {
    const createRes = await fetch(`${httpBase}/api/documents`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: 'Replace suggestion regression',
        markdown: 'Alpha beta gamma',
        marks: {},
      }),
    });
    const created = await mustJson<{ slug: string; ownerSecret: string; accessToken: string }>(createRes, 'create');

    const suggestRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/suggest-replace`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({
        quote: 'beta',
        content: 'delta',
        by: 'human:Anonymous',
      }),
    });
    const suggest = await mustJson<{ marks?: Record<string, { kind?: string; quote?: string; content?: string; originalQuote?: string }> }>(suggestRes, 'suggest-replace');
    const suggestionId = Object.keys(suggest.marks ?? {})[0] ?? '';
    assert(suggestionId.length > 0, 'Expected suggestion id');

    const stateRes = await fetch(`${httpBase}/api/agent/${created.slug}/state`, {
      headers: {
        ...CLIENT_HEADERS,
        Authorization: `Bearer ${created.accessToken}`,
      },
    });
    const state = await mustJson<{
      markdown?: string;
      marks?: Record<string, { kind?: string; quote?: string; content?: string; originalQuote?: string }>;
    }>(stateRes, 'state');

    assert(
      (state.markdown ?? '').includes('delta'),
      `Expected pending replace suggestion to land as live replacement text. markdown=${String(state.markdown)}`,
    );
    assert(
      !((state.markdown ?? '').includes('beta')),
      `Expected original replacement text to stay out of canonical markdown. markdown=${String(state.markdown)}`,
    );
    assert(
      state.marks?.[suggestionId]?.kind === 'replace'
        && state.marks?.[suggestionId]?.quote === 'delta'
        && state.marks?.[suggestionId]?.content === 'delta'
        && state.marks?.[suggestionId]?.originalQuote === 'beta',
      `Expected replace metadata to preserve both edited and original text. marks=${JSON.stringify(state.marks ?? {})}`,
    );

    const acceptRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({
        markId: suggestionId,
        by: 'human:Anonymous',
      }),
    });
    const accepted = await mustJson<{ success?: boolean; marks?: Record<string, unknown>; markdown?: string }>(acceptRes, 'accept');
    assert(accepted.success === true, `Expected accept to succeed. response=${JSON.stringify(accepted)}`);
    assert(
      Object.keys(accepted.marks ?? {}).length === 0,
      `Expected accept response to omit finalized suggestion metadata. marks=${JSON.stringify(accepted.marks ?? {})}`,
    );
    assert(
      (accepted.markdown ?? '').includes('delta'),
      `Expected accept response to preserve accepted replacement text. markdown=${String(accepted.markdown)}`,
    );

    const stateAfterAcceptRes = await fetch(`${httpBase}/api/agent/${created.slug}/state`, {
      headers: {
        ...CLIENT_HEADERS,
        Authorization: `Bearer ${created.accessToken}`,
      },
    });
    const stateAfterAccept = await mustJson<{
      markdown?: string;
      marks?: Record<string, unknown>;
    }>(stateAfterAcceptRes, 'state-after-accept');
    assert(
      (stateAfterAccept.markdown ?? '').includes('delta'),
      `Expected accepted replacement text to remain in canonical markdown. markdown=${String(stateAfterAccept.markdown)}`,
    );
    assert(
      Object.keys(stateAfterAccept.marks ?? {}).length === 0,
      `Expected accepted suggestion to be removed from canonical state marks. marks=${JSON.stringify(stateAfterAccept.marks ?? {})}`,
    );

    console.log('✓ API replace suggestions now persist as live replacement text and accept returns a finalized visible-marks view');
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await collab.stopCollabRuntime();
    wss.close();
    if (previousDbPath === undefined) {
      delete process.env.DATABASE_PATH;
    } else {
      process.env.DATABASE_PATH = previousDbPath;
    }
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        unlinkSync(`${dbPath}${suffix}`);
      } catch {
        // ignore cleanup errors
      }
    }
  }
}

run().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exit(1);
});
