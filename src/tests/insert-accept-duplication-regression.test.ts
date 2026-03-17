import { unlinkSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(`FAIL: ${message}`);
}

async function run(): Promise<void> {
  const dbName = `proof-insert-accept-dup-${Date.now()}-${randomUUID()}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  process.env.DATABASE_PATH = dbPath;
  process.env.COLLAB_EMBEDDED_WS = '0';

  const { executeDocumentOperation } = await import('../../server/document-engine.js');
  const db = await import('../../server/db.js');

  const createDoc = (slug: string, markdown: string, marks: Record<string, unknown>) => {
    db.createDocument(slug, markdown, marks, `Test ${slug}`);
  };

  try {
    {
      const slug = `insert-dup-${randomUUID().slice(0, 8)}`;
      const markId = randomUUID();
      createDoc(slug, 'Alpha beta gamma. delta', {
        [markId]: {
          kind: 'insert',
          by: 'user:test',
          createdAt: new Date().toISOString(),
          status: 'pending',
          content: ' delta',
          quote: 'delta',
        },
      });

      const accept = executeDocumentOperation(slug, 'POST', '/marks/accept', { markId, by: 'user:test' });
      assert(accept.status === 200, `Expected plain insert accept to succeed, got ${accept.status}`);
      assert(
        Object.keys((accept.body.marks as Record<string, unknown>) ?? {}).length === 0,
        'Expected accept response to expose no remaining visible insert marks',
      );

      const state = executeDocumentOperation(slug, 'GET', '/state');
      const markdown = String(state.body.markdown ?? '');
      const cleanMarkdown = markdown.replace(/<\/?span\b[^>]*>/gi, '');
      const deltaCount = (cleanMarkdown.match(/delta/g) || []).length;

      assert(deltaCount === 1, `Expected accepted insert text once, saw ${deltaCount}: ${cleanMarkdown}`);
    }

    {
      const slug = `insert-span-${randomUUID().slice(0, 8)}`;
      const markId = randomUUID();
      createDoc(
        slug,
        `First sentence.<span data-proof="authored" data-id="${markId}"> inserted text</span> End.`,
        {
          [markId]: {
            kind: 'insert',
            by: 'user:test',
            createdAt: new Date().toISOString(),
            status: 'pending',
            content: ' inserted text',
            quote: 'inserted text',
          },
        },
      );

      const accept = executeDocumentOperation(slug, 'POST', '/marks/accept', { markId, by: 'user:test' });
      assert(accept.status === 200, `Expected span-wrapped insert accept to succeed, got ${accept.status}`);
      assert(
        Object.keys((accept.body.marks as Record<string, unknown>) ?? {}).length === 0,
        'Expected span-wrapped accept response to expose no remaining visible insert marks',
      );

      const state = executeDocumentOperation(slug, 'GET', '/state');
      const markdown = String(state.body.markdown ?? '');
      const cleanMarkdown = markdown.replace(/<\/?span\b[^>]*>/gi, '');
      const insertCount = (cleanMarkdown.match(/inserted text/g) || []).length;

      assert(insertCount === 1, `Expected accepted span text once, saw ${insertCount}: ${cleanMarkdown}`);
      assert(!markdown.includes(`data-id="${markId}"`), `Expected accepted insert span to be stripped: ${markdown}`);
    }

    console.log('PASS insert-accept duplication regression');
  } finally {
    try {
      unlinkSync(dbPath);
    } catch {
      // best-effort cleanup
    }
  }
}

void run().catch((error) => {
  console.error(error);
  process.exit(1);
});
