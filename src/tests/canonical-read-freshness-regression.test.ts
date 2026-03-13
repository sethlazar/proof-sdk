import { unlinkSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import express from 'express';
import * as Y from 'yjs';

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
  const dbName = `proof-canonical-read-freshness-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);

  const prevDatabasePath = process.env.DATABASE_PATH;
  const prevProofEnv = process.env.PROOF_ENV;
  const prevNodeEnv = process.env.NODE_ENV;
  const prevDbEnvInit = process.env.PROOF_DB_ENV_INIT;
  const prevEditV2Enabled = process.env.AGENT_EDIT_V2_ENABLED;

  process.env.DATABASE_PATH = dbPath;
  process.env.PROOF_ENV = 'development';
  process.env.NODE_ENV = 'development';
  process.env.AGENT_EDIT_V2_ENABLED = '1';
  delete process.env.PROOF_DB_ENV_INIT;

  const [{ apiRoutes }, { agentRoutes }, { shareWebRoutes }, db, collab] = await Promise.all([
    import('../../server/routes.js'),
    import('../../server/agent-routes.js'),
    import('../../server/share-web-routes.js'),
    import('../../server/db.js'),
    import('../../server/collab.js'),
  ]);

  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.use('/api', apiRoutes);
  app.use('/api/agent', agentRoutes);
  app.use('/', shareWebRoutes);

  const server = createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  const address = server.address() as AddressInfo;
  const httpBase = `http://127.0.0.1:${address.port}`;

  try {
    let baseMarkdown = '# Long QA\n\n';
    for (let index = 1; index <= 230; index += 1) {
      baseMarkdown += `## Section ${index}\nline ${index}\n\n`;
    }
    const markerA = 'canonical-read-marker-a';
    const markerB = 'canonical-read-marker-b';
    const canonicalMarkdown = `${baseMarkdown}${markerA}\n\n${markerB}\n`;
    const commentId = 'comment-1';
    const seededMarks = {
      [commentId]: {
        kind: 'comment',
        by: 'qa:canonical-read',
        createdAt: '2026-03-07T00:00:00.000Z',
        quote: 'Section 5',
        text: 'Preserve this comment mark during fallback reads.',
        threadId: commentId,
        thread: [],
        resolved: false,
      },
    };

    const createRes = await fetch(`${httpBase}/api/documents`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        title: 'canonical read freshness',
        markdown: baseMarkdown,
        marks: seededMarks,
      }),
    });
    const created = await mustJson<{ slug: string; ownerSecret: string }>(createRes, 'create');

    const ydoc = new Y.Doc();
    ydoc.getText('markdown').insert(0, canonicalMarkdown);
    const snapshot = Y.encodeStateAsUpdate(ydoc);
    db.saveYSnapshot(created.slug, 1, snapshot);
    db.getDb().prepare(`
      UPDATE documents
      SET y_state_version = 1
      WHERE slug = ?
    `).run(created.slug);

    const projectionBeforeReads = db.getDocumentProjectionBySlug(created.slug);
    assert(Boolean(projectionBeforeReads), 'Expected projection row');
    assert(projectionBeforeReads?.y_state_version === 0, `Expected stale projection y_state_version=0, got ${String(projectionBeforeReads?.y_state_version)}`);

    const staleDocs = db.listDocsWithStaleProjection(20);
    assert(staleDocs.some((candidate) => candidate.slug === created.slug), 'Expected stale projection candidate to include test slug');

    const suspiciousDocs = db.listSuspiciousProjectionCandidates(20, 1);
    assert(suspiciousDocs.some((candidate) => candidate.slug === created.slug), 'Expected suspicious projection candidates to include test slug');

    const stateRes = await fetch(`${httpBase}/api/agent/${created.slug}/state`, {
      headers: {
        ...CLIENT_HEADERS,
        'x-share-token': created.ownerSecret,
      },
    });
    const state = await mustJson<{
      success: boolean;
      markdown?: string;
      content?: string;
      marks?: Record<string, { kind?: string }>;
      readSource?: string;
      projectionFresh?: boolean;
      mutationReady?: boolean;
      revision?: number | null;
      updatedAt?: string | null;
      _links?: Record<string, unknown>;
      agent?: Record<string, unknown>;
      warning?: { code?: string };
    }>(stateRes, 'state');
    const stateMarkdown = typeof state.markdown === 'string' ? state.markdown : (state.content ?? '');
    assert(stateMarkdown.includes(markerA), 'Expected /state to serve canonical marker A from Yjs fallback');
    assert(stateMarkdown.includes(markerB), 'Expected /state to serve canonical marker B from Yjs fallback');
    assert(stateMarkdown.includes('Section 230'), 'Expected /state to preserve Section 230 content');
    assert(state.readSource === 'yjs_fallback', `Expected yjs_fallback readSource, got ${String(state.readSource)}`);
    assert(state.projectionFresh === false, 'Expected projectionFresh=false during fallback reads');
    assert(state.mutationReady === false, 'Expected mutationReady=false during fallback reads');
    assert(state.revision === null, `Expected null revision during fallback read, got ${String(state.revision)}`);
    assert(state.updatedAt === null, `Expected null updatedAt during fallback read, got ${String(state.updatedAt)}`);
    assert(state.warning?.code === 'PROJECTION_STALE', `Expected PROJECTION_STALE warning, got ${String(state.warning?.code)}`);
    assert(state.marks?.[commentId]?.kind === 'comment', 'Expected fallback read to preserve DB comment mark');
    assert(!state._links?.editV2, 'Expected /state fallback to withhold editV2 link');
    assert(!(state.agent && 'editV2Api' in state.agent), 'Expected /state fallback to withhold agent.editV2Api');

    const snapshotRes = await fetch(`${httpBase}/api/agent/${created.slug}/snapshot`, {
      headers: {
        ...CLIENT_HEADERS,
        'x-share-token': created.ownerSecret,
      },
    });
    const snapshotBody = await mustJson<{
      success: boolean;
      blocks?: Array<{ markdown?: string }>;
      readSource?: string;
      projectionFresh?: boolean;
      mutationReady?: boolean;
      revision?: number | null;
      _links?: Record<string, unknown>;
      warning?: { code?: string };
    }>(snapshotRes, 'snapshot');
    const snapshotMarkdown = Array.isArray(snapshotBody.blocks)
      ? snapshotBody.blocks.map((block) => String(block.markdown ?? '')).join('\n')
      : '';
    assert(snapshotMarkdown.includes(markerA), 'Expected /snapshot to serve canonical marker A from Yjs fallback');
    assert(snapshotMarkdown.includes(markerB), 'Expected /snapshot to serve canonical marker B from Yjs fallback');
    assert(snapshotBody.readSource === 'yjs_fallback', `Expected snapshot readSource=yjs_fallback, got ${String(snapshotBody.readSource)}`);
    assert(snapshotBody.projectionFresh === false, 'Expected snapshot projectionFresh=false during fallback reads');
    assert(snapshotBody.mutationReady === false, 'Expected snapshot mutationReady=false during fallback reads');
    assert(snapshotBody.revision === null, `Expected snapshot revision=null during fallback reads, got ${String(snapshotBody.revision)}`);
    assert(snapshotBody.warning?.code === 'PROJECTION_STALE', `Expected snapshot PROJECTION_STALE warning, got ${String(snapshotBody.warning?.code)}`);
    assert(!snapshotBody._links?.editV2, 'Expected snapshot fallback to withhold editV2 link');

    const editV2Res = await fetch(`${httpBase}/api/agent/${created.slug}/edit/v2`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({
        by: 'qa:canonical-read',
        baseRevision: 1,
        operations: [
          { op: 'replace_block', ref: 'b1', block: { markdown: '# Long QA' } },
        ],
      }),
    });
    assert(editV2Res.status === 409, `Expected edit/v2 fallback 409, got ${editV2Res.status}`);
    const editV2Body = JSON.parse(await editV2Res.text()) as { success?: boolean; code?: string };
    assert(editV2Body.code === 'PROJECTION_STALE', `Expected PROJECTION_STALE from edit/v2, got ${String(editV2Body.code)}`);

    const staleMutationCases: Array<{ path: string; body: Record<string, unknown> }> = [
      {
        path: '/marks/comment',
        body: {
          by: 'qa:canonical-read',
          quote: 'Section 5',
          text: 'Should be rejected while projection is stale',
        },
      },
      {
        path: '/marks/suggest-replace',
        body: {
          by: 'qa:canonical-read',
          quote: 'Section 5',
          content: 'Replacement text',
        },
      },
      {
        path: '/marks/reply',
        body: {
          by: 'qa:canonical-read',
          markId: commentId,
          text: 'Should be rejected while projection is stale',
        },
      },
      {
        path: '/marks/resolve',
        body: {
          by: 'qa:canonical-read',
          markId: commentId,
        },
      },
      {
        path: '/marks/unresolve',
        body: {
          by: 'qa:canonical-read',
          markId: commentId,
        },
      },
    ];
    for (const mutationCase of staleMutationCases) {
      const mutationRes = await fetch(`${httpBase}/api/agent/${created.slug}${mutationCase.path}`, {
        method: 'POST',
        headers: {
          ...CLIENT_HEADERS,
          'Content-Type': 'application/json',
          'x-share-token': created.ownerSecret,
        },
        body: JSON.stringify(mutationCase.body),
      });
      assert(mutationRes.status === 409, `Expected ${mutationCase.path} fallback 409, got ${mutationRes.status}`);
      const mutationBody = JSON.parse(await mutationRes.text()) as { code?: string };
      assert(mutationBody.code === 'PROJECTION_STALE', `Expected PROJECTION_STALE from ${mutationCase.path}, got ${String(mutationBody.code)}`);
    }

    const shareRes = await fetch(`${httpBase}/d/${created.slug}?token=${encodeURIComponent(created.ownerSecret)}`, {
      headers: {
        ...CLIENT_HEADERS,
        Accept: 'application/json',
      },
    });
    const shareBody = await mustJson<{
      success: boolean;
      markdown?: string | null;
      readSource?: string | null;
      projectionFresh?: boolean | null;
      mutationReady?: boolean | null;
      _links?: Record<string, unknown>;
      agent?: Record<string, unknown>;
      warning?: { code?: string };
    }>(shareRes, 'share json');
    const shareMarkdown = typeof shareBody.markdown === 'string' ? shareBody.markdown : '';
    assert(shareMarkdown.includes(markerA), 'Expected share JSON to serve canonical marker A from Yjs fallback');
    assert(shareMarkdown.includes(markerB), 'Expected share JSON to serve canonical marker B from Yjs fallback');
    assert(shareBody.readSource === 'yjs_fallback', `Expected share readSource=yjs_fallback, got ${String(shareBody.readSource)}`);
    assert(shareBody.projectionFresh === false, 'Expected share projectionFresh=false during fallback reads');
    assert(shareBody.mutationReady === false, 'Expected share mutationReady=false during fallback reads');
    assert(shareBody.warning?.code === 'PROJECTION_STALE', `Expected share PROJECTION_STALE warning, got ${String(shareBody.warning?.code)}`);
    assert(!shareBody._links?.editV2, 'Expected share fallback to withhold editV2 link');
    assert(!(shareBody.agent && 'editV2Api' in shareBody.agent), 'Expected share fallback to withhold agent.editV2Api');

    const createSuggestionFixture = async (
      title: string,
      markId: string,
    ): Promise<{ slug: string; ownerSecret: string; markId: string }> => {
      const slug = Math.random().toString(36).slice(2, 10);
      const ownerSecret = `owner-${slug}`;
      const liveReplaceMark = {
        kind: 'replace',
        by: 'qa:canonical-read',
        createdAt: '2026-03-07T00:00:00.000Z',
        quote: 'delta',
        content: 'delta',
        originalQuote: 'beta',
        status: 'pending',
        startRel: 'char:6',
        endRel: 'char:11',
        range: { from: 7, to: 12 },
      };
      db.createDocument(
        slug,
        `Alpha <span data-proof="suggestion" data-id="${markId}" data-by="qa:canonical-read" data-kind="replace">delta</span> gamma.`,
        {
          [markId]: liveReplaceMark,
        },
        title,
        undefined,
        ownerSecret,
      );

      const suggestionYDoc = new Y.Doc();
      suggestionYDoc.getText('markdown').insert(0, 'Alpha delta gamma.');
      suggestionYDoc.getMap('marks').set(markId, liveReplaceMark);
      db.saveYSnapshot(slug, 1, Y.encodeStateAsUpdate(suggestionYDoc));
      db.getDb().prepare(`
        UPDATE documents
        SET y_state_version = 1
        WHERE slug = ?
      `).run(slug);

      return { slug, ownerSecret, markId };
    };

    const acceptFixture = await createSuggestionFixture('canonical read accept fallback', 'replace-live-fallback-accept');
    const acceptBlockedRes = await fetch(`${httpBase}/api/agent/${acceptFixture.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': acceptFixture.ownerSecret,
      },
      body: JSON.stringify({
        by: 'qa:canonical-read',
        markId: acceptFixture.markId,
      }),
    });
    assert(acceptBlockedRes.status === 409, `Expected accept to block before live collab is loaded, got ${acceptBlockedRes.status}`);
    const acceptBlockedBody = JSON.parse(await acceptBlockedRes.text()) as { code?: string };
    assert(acceptBlockedBody.code === 'PROJECTION_STALE', `Expected PROJECTION_STALE before live collab load, got ${String(acceptBlockedBody.code)}`);

    const loadedAcceptDoc = await collab.loadCanonicalYDoc(acceptFixture.slug);
    assert(loadedAcceptDoc, 'Expected accept fixture canonical Yjs doc to load');
    const acceptRow = db.getDocumentBySlug(acceptFixture.slug);
    assert(acceptRow, 'Expected accept fixture row to exist');
    collab.registerCanonicalYDocPersistence(acceptFixture.slug, loadedAcceptDoc.ydoc, {
      updatedAt: acceptRow.updated_at,
      yStateVersion: acceptRow.y_state_version,
      accessEpoch: acceptRow.access_epoch,
    });

    const acceptAllowedRes = await fetch(`${httpBase}/api/agent/${acceptFixture.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': acceptFixture.ownerSecret,
      },
      body: JSON.stringify({
        by: 'qa:canonical-read',
        markId: acceptFixture.markId,
      }),
    });
    assert(acceptAllowedRes.status === 200, `Expected accept to succeed with loaded live collab doc, got ${acceptAllowedRes.status}`);
    const acceptedDoc = db.getDocumentBySlug(acceptFixture.slug);
    assert(acceptedDoc?.markdown.includes('Alpha delta gamma.'), 'Expected accept to persist the replacement through live fallback');
    const acceptedMarks = JSON.parse(acceptedDoc?.marks ?? '{}') as Record<string, { status?: string }>;
    assert(acceptedMarks[acceptFixture.markId]?.status !== 'pending', 'Expected accept to clear the pending suggestion mark state');

    const rejectFixture = await createSuggestionFixture('canonical read reject fallback', 'replace-live-fallback-reject');
    const loadedRejectDoc = await collab.loadCanonicalYDoc(rejectFixture.slug);
    assert(loadedRejectDoc, 'Expected reject fixture canonical Yjs doc to load');
    const rejectRow = db.getDocumentBySlug(rejectFixture.slug);
    assert(rejectRow, 'Expected reject fixture row to exist');
    collab.registerCanonicalYDocPersistence(rejectFixture.slug, loadedRejectDoc.ydoc, {
      updatedAt: rejectRow.updated_at,
      yStateVersion: rejectRow.y_state_version,
      accessEpoch: rejectRow.access_epoch,
    });

    const rejectAllowedRes = await fetch(`${httpBase}/api/agent/${rejectFixture.slug}/marks/reject`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': rejectFixture.ownerSecret,
      },
      body: JSON.stringify({
        by: 'qa:canonical-read',
        markId: rejectFixture.markId,
      }),
    });
    assert(rejectAllowedRes.status === 200, `Expected reject to succeed with loaded live collab doc, got ${rejectAllowedRes.status}`);
    const rejectedDoc = db.getDocumentBySlug(rejectFixture.slug);
    assert(rejectedDoc?.markdown.includes('Alpha beta gamma.'), 'Expected reject to preserve the original text through live fallback');
    const rejectedMarks = JSON.parse(rejectedDoc?.marks ?? '{}') as Record<string, { status?: string }>;
    assert(rejectedMarks[rejectFixture.markId]?.status !== 'pending', 'Expected reject to clear the pending suggestion mark state');

    const projectionAfterReads = db.getDocumentProjectionBySlug(created.slug);
    assert(projectionAfterReads?.y_state_version === 0, `Expected reads to avoid mutating stale projection y_state_version, got ${String(projectionAfterReads?.y_state_version)}`);

    console.log('✓ canonical read paths fall back to Yjs state read-only while preserving action marks when the projection row is stale');
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));

    if (prevDatabasePath === undefined) delete process.env.DATABASE_PATH;
    else process.env.DATABASE_PATH = prevDatabasePath;

    if (prevProofEnv === undefined) delete process.env.PROOF_ENV;
    else process.env.PROOF_ENV = prevProofEnv;

    if (prevNodeEnv === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = prevNodeEnv;

    if (prevDbEnvInit === undefined) delete process.env.PROOF_DB_ENV_INIT;
    else process.env.PROOF_DB_ENV_INIT = prevDbEnvInit;

    if (prevEditV2Enabled === undefined) delete process.env.AGENT_EDIT_V2_ENABLED;
    else process.env.AGENT_EDIT_V2_ENABLED = prevEditV2Enabled;

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
