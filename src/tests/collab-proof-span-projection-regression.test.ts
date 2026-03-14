import { unlinkSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import * as Y from 'yjs';
import { prosemirrorToYXmlFragment } from 'y-prosemirror';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function run(): Promise<void> {
  const dbName = `proof-collab-proof-span-projection-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  const previousDbPath = process.env.DATABASE_PATH;
  process.env.DATABASE_PATH = dbPath;

  const db = await import('../../server/db.ts');
  const collab = await import('../../server/collab.ts');
  const { getHeadlessMilkdownParser, parseMarkdownWithHtmlFallback } = await import('../../server/milkdown-headless.ts');

  const slug = `proof-span-projection-${Math.random().toString(36).slice(2, 10)}`;
  const suggestionId = `replace-${Math.random().toString(36).slice(2, 10)}`;
  const canonicalMarkdown = 'Alpha delta gamma';
  const fragmentMarkdown = `Alpha <span data-proof="suggestion" data-id="${suggestionId}" data-by="human:Anonymous" data-kind="replace" data-content="delta">beta</span> gamma`;
  const suggestionMarks = {
    [suggestionId]: {
      kind: 'replace',
      by: 'human:Anonymous',
      createdAt: new Date().toISOString(),
      quote: 'delta',
      status: 'pending',
      content: 'delta',
      originalQuote: 'beta',
      startRel: 'char:6',
      endRel: 'char:11',
    },
  };

  try {
    db.createDocument(slug, canonicalMarkdown, suggestionMarks, 'proof span projection regression');

    await collab.startCollabRuntimeEmbedded(4000);
    const instance = collab.__unsafeGetHocuspocusInstanceForTests() as {
      createDocument?: (
        slug: string,
        request: Record<string, unknown>,
        socketId: string,
        context: Record<string, unknown>,
        hooks: Record<string, unknown>,
      ) => Promise<Y.Doc>;
    };
    assert(instance && typeof instance.createDocument === 'function', 'Expected collab test instance');

    await instance.createDocument(
      slug,
      {},
      'proof-span-projection-socket',
      { isAuthenticated: true, readOnly: false, requiresAuthentication: true },
      {},
    );
    const loadedDoc = collab.__unsafeGetLoadedDocForTests(slug);
    assert(Boolean(loadedDoc), 'Expected live loaded doc after collab load');

    const parser = await getHeadlessMilkdownParser();
    const parsed = parseMarkdownWithHtmlFallback(parser, fragmentMarkdown);
    assert(Boolean(parsed.doc), `Expected proof span markdown to parse, got mode=${parsed.mode}`);

    loadedDoc!.transact(() => {
      const markdownText = loadedDoc!.getText('markdown');
      if (markdownText.length > 0) markdownText.delete(0, markdownText.length);
      markdownText.insert(0, canonicalMarkdown);

      const marksMap = loadedDoc!.getMap('marks');
      for (const key of Array.from(marksMap.keys())) {
        marksMap.delete(key);
      }
      for (const [id, mark] of Object.entries(suggestionMarks)) {
        marksMap.set(id, mark as unknown);
      }

      const fragment = loadedDoc!.getXmlFragment('prosemirror');
      const length = fragment.length;
      if (length > 0) fragment.delete(0, length);
      prosemirrorToYXmlFragment(parsed.doc as any, fragment as any);
    }, 'proof-span-fragment-seed');

    const derivedMarkdown = await collab.getLoadedCollabMarkdownFromFragment(slug);
    assert(
      (derivedMarkdown ?? '').trim() === canonicalMarkdown,
      `Expected fragment-derived markdown to strip proof spans back to semantic text. markdown=${String(derivedMarkdown)}`,
    );

    const safety = collab.evaluateProjectionSafety(canonicalMarkdown, canonicalMarkdown, loadedDoc!);
    assert(
      safety.safe,
      `Expected projection safety guard to allow canonical live replacement text. safety=${JSON.stringify(safety)}`,
    );

    collab.__unsafePersistDocForTests(slug, loadedDoc!, 'proof-span-fragment-persist');
    await sleep(250);

    const row = db.getDocumentBySlug(slug);
    assert(Boolean(row), 'Expected canonical row after collab persist');
    assert(
      (row?.markdown ?? '').trim() === canonicalMarkdown,
      `Expected canonical markdown to stay semantic instead of span-inflated. markdown=${String(row?.markdown)}`,
    );

    const persistedMarks = JSON.parse(row?.marks ?? '{}') as Record<string, {
      kind?: string;
      content?: string;
      quote?: string;
      originalQuote?: string;
    }>;
    assert(
      persistedMarks[suggestionId]?.kind === 'replace'
        && persistedMarks[suggestionId]?.content === 'delta'
        && persistedMarks[suggestionId]?.quote === 'delta'
        && persistedMarks[suggestionId]?.originalQuote === 'beta',
      `Expected pending replace suggestion metadata to survive collab persist. marks=${row?.marks ?? '{}'}`,
    );

    console.log('✓ collab projection guard accepts semantic live replacements while proof spans are present in the fragment');
  } finally {
    if (previousDbPath === undefined) {
      delete process.env.DATABASE_PATH;
    } else {
      process.env.DATABASE_PATH = previousDbPath;
    }
    await collab.stopCollabRuntime();
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
