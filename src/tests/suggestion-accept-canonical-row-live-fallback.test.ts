import { unlinkSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as Y from 'yjs';
import { canonicalizeStoredMarks, type StoredMark } from '../formats/marks.js';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

function parseStoredMarks(raw: unknown): Record<string, StoredMark> {
  if (typeof raw !== 'string') return {};
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
    return canonicalizeStoredMarks(parsed as Record<string, StoredMark>);
  } catch {
    return {};
  }
}

function buildInsertMark(markdown: string, quote: string, by: string, createdAt: string): StoredMark {
  const content = ` ${quote}`;
  const start = markdown.indexOf(content);
  if (start < 0) {
    throw new Error(`Could not find visible insert content "${content}" in markdown`);
  }
  return {
    kind: 'insert',
    by,
    createdAt,
    status: 'pending',
    content,
    quote,
    range: {
      from: start + 1,
      to: start + 1 + content.length,
    },
    startRel: `char:${start}`,
    endRel: `char:${start + content.length}`,
  } satisfies StoredMark;
}

async function run(): Promise<void> {
  const dbName = `proof-suggestion-accept-live-fallback-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);

  const prevDatabasePath = process.env.DATABASE_PATH;
  const prevProofEnv = process.env.PROOF_ENV;
  const prevNodeEnv = process.env.NODE_ENV;
  const prevDbEnvInit = process.env.PROOF_DB_ENV_INIT;

  process.env.DATABASE_PATH = dbPath;
  process.env.PROOF_ENV = 'development';
  process.env.NODE_ENV = 'development';
  delete process.env.PROOF_DB_ENV_INIT;

  const db = await import('../../server/db.ts');
  const { executeDocumentOperationAsync } = await import('../../server/document-engine.ts');

  try {
    const slug = `accept-live-fallback-${Math.random().toString(36).slice(2, 10)}`;
    const createdAt = new Date('2026-03-17T01:00:00.000Z').toISOString();
    const markdown = 'Alpha One.\nBeta Two.\n';
    const firstMarkId = 'insert-one';
    const secondMarkId = 'insert-two';
    const firstMark = buildInsertMark(markdown, 'One', 'human:test', createdAt);
    const secondMark = buildInsertMark(markdown, 'Two', 'human:test', createdAt);

    db.createDocument(
      slug,
      markdown,
      canonicalizeStoredMarks({
        [firstMarkId]: firstMark,
      }),
      'Accept live fallback regression',
    );

    const now = new Date().toISOString();
    db.getDb().prepare(`
      UPDATE documents
      SET revision = 2, y_state_version = 1, updated_at = ?
      WHERE slug = ?
    `).run(now, slug);

    db.getDb().prepare(`
      UPDATE document_projections
      SET markdown = ?, marks_json = ?, revision = 2, y_state_version = 1, updated_at = ?, health = 'healthy'
      WHERE document_slug = ?
    `).run('Projection stale body.\n', JSON.stringify({ [firstMarkId]: firstMark }), now, slug);

    const ydoc = new Y.Doc();
    ydoc.getText('markdown').insert(0, markdown);
    ydoc.getMap('marks').set(firstMarkId, firstMark);
    ydoc.getMap('marks').set(secondMarkId, secondMark);
    db.saveYSnapshot(slug, 1, Y.encodeStateAsUpdate(ydoc));

    const acceptResult = await executeDocumentOperationAsync(slug, 'POST', '/marks/accept', {
      markId: firstMarkId,
      by: 'human:test',
    });
    assert(acceptResult.status === 200, `Expected accept to succeed, got ${acceptResult.status}`);

    const responseMarks = (
      (acceptResult.body as { marks?: Record<string, StoredMark> }).marks
      ?? {}
    );
    assert(
      !Object.prototype.hasOwnProperty.call(responseMarks, firstMarkId),
      'Expected accepted mark to be removed from the response marks payload',
    );
    assert(
      Object.prototype.hasOwnProperty.call(responseMarks, secondMarkId),
      'Expected accept response to preserve pending live suggestions missing from the canonical row base',
    );

    const updatedDoc = db.getDocumentBySlug(slug);
    const updatedMarks = parseStoredMarks(updatedDoc?.marks);
    assert(
      !Object.prototype.hasOwnProperty.call(updatedMarks, firstMarkId),
      'Expected canonical document to drop the accepted suggestion mark',
    );
    assert(
      Object.prototype.hasOwnProperty.call(updatedMarks, secondMarkId),
      'Expected canonical document to keep the other pending live suggestion after accepting one mark',
    );

    console.log('✓ marks/accept prefers loaded collab state when canonical row omits other pending live suggestions');
  } finally {
    if (prevDatabasePath === undefined) delete process.env.DATABASE_PATH;
    else process.env.DATABASE_PATH = prevDatabasePath;
    if (prevProofEnv === undefined) delete process.env.PROOF_ENV;
    else process.env.PROOF_ENV = prevProofEnv;
    if (prevNodeEnv === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = prevNodeEnv;
    if (prevDbEnvInit === undefined) delete process.env.PROOF_DB_ENV_INIT;
    else process.env.PROOF_DB_ENV_INIT = prevDbEnvInit;

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
