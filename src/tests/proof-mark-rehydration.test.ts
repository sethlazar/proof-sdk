import { unlinkSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { canonicalizeStoredMarks, type StoredMark } from '../formats/marks.js';
import { stripAllProofSpanTags, stripProofSpanTags } from '../../server/proof-span-strip.ts';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

function assertEqual<T>(actual: T, expected: T, message?: string): void {
  if (actual !== expected) {
    throw new Error(message ?? `Expected ${String(expected)}, got ${String(actual)}`);
  }
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

function buildRelativeAnchors(baseMarkdown: string, quote: string): { startRel: string; endRel: string; range: { from: number; to: number } } {
  const start = baseMarkdown.indexOf(quote);
  if (start < 0) {
    throw new Error(`Quote not found in base markdown: ${quote}`);
  }
  return {
    startRel: `char:${start}`,
    endRel: `char:${start + quote.length}`,
    range: {
      from: start + 1,
      to: start + 1 + Math.min(100, quote.length),
    },
  };
}

async function run(): Promise<void> {
  const dbName = `proof-mark-rehydration-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
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
  const { rehydrateProofMarksMarkdown, finalizeSuggestionThroughRehydration } = await import('../../server/proof-mark-rehydration.ts');
  const { repairProofMarksForSlug } = await import('../../server/proof-mark-repair.ts');

  try {
    const createdAt = new Date('2026-03-10T18:00:00.000Z').toISOString();

    const acceptSlug = `rehydrate-accept-${Math.random().toString(36).slice(2, 10)}`;
    const fullQuote = 'You can try it yourself right now. A copy of this post is available on Proof. Use the share link there to have Claude, ChatGPT, your claw, or any other agent add their comments.';
    const truncatedQuote = fullQuote.slice(0, 100);
    const replacement = 'Proof lets you share a doc with an agent and review its edits inline.';
    const acceptBase = `# Launch\n\n${fullQuote}\n\n## Next`;
    const acceptAnchors = buildRelativeAnchors(acceptBase, fullQuote);
    const acceptMarkId = 'legacy-replace-accept';
    db.createDocument(
      acceptSlug,
      `# Launch\n\n<span data-proof="suggestion" data-id="${acceptMarkId}" data-by="ai:test" data-kind="replace">${truncatedQuote}</span>\n\n## Next`,
      canonicalizeStoredMarks({
        [acceptMarkId]: {
          kind: 'replace',
          by: 'ai:test',
          createdAt,
          quote: fullQuote,
          content: replacement,
          status: 'pending',
          startRel: acceptAnchors.startRel,
          endRel: acceptAnchors.endRel,
          range: acceptAnchors.range,
        } satisfies StoredMark,
      }),
      'Legacy accept repair',
    );

    const acceptResult = await executeDocumentOperationAsync(acceptSlug, 'POST', '/marks/accept', {
      markId: acceptMarkId,
      by: 'human:test',
    });
    assertEqual(acceptResult.status, 200, `Expected legacy accept to succeed, got ${acceptResult.status}`);
    const acceptedDoc = db.getDocumentBySlug(acceptSlug);
    assert(acceptedDoc?.markdown.includes(replacement), 'Expected accept to write replacement content into canonical markdown');
    assert(!acceptedDoc?.markdown.includes(truncatedQuote), 'Expected accept to remove the stale truncated wrapper text');
    assert(!acceptedDoc?.markdown.includes('data-proof="suggestion"'), 'Expected accepted suggestion wrapper to be removed');

    const rejectSlug = `rehydrate-reject-${Math.random().toString(36).slice(2, 10)}`;
    const rejectMarkId = 'legacy-replace-reject';
    db.createDocument(
      rejectSlug,
      `# Launch\n\n<span data-proof="suggestion" data-id="${rejectMarkId}" data-by="ai:test" data-kind="replace">${truncatedQuote}</span>\n\n## Next`,
      canonicalizeStoredMarks({
        [rejectMarkId]: {
          kind: 'replace',
          by: 'ai:test',
          createdAt,
          quote: fullQuote,
          content: replacement,
          status: 'pending',
          startRel: acceptAnchors.startRel,
          endRel: acceptAnchors.endRel,
          range: acceptAnchors.range,
        } satisfies StoredMark,
      }),
      'Legacy reject repair',
    );

    const rejectResult = await executeDocumentOperationAsync(rejectSlug, 'POST', '/marks/reject', {
      markId: rejectMarkId,
      by: 'human:test',
    });
    assertEqual(rejectResult.status, 200, `Expected legacy reject to succeed, got ${rejectResult.status}`);
    const rejectedDoc = db.getDocumentBySlug(rejectSlug);
    const rejectedVisibleText = stripAllProofSpanTags(rejectedDoc?.markdown ?? '');
    assert(rejectedVisibleText.includes(fullQuote), 'Expected reject to restore the full original quote text');
    assert(!rejectedDoc?.markdown.includes('data-proof="suggestion"'), 'Expected rejected suggestion wrapper to be removed');

    const entityInsertMarkId = 'entity-insert-accept';
    const entityInsertResult = await finalizeSuggestionThroughRehydration({
      markdown: 'Alpha beta gamma. delta&#x20;',
      marks: canonicalizeStoredMarks({
        [entityInsertMarkId]: {
          kind: 'insert',
          by: 'human:test',
          createdAt,
          quote: 'delta',
          content: ' delta',
          status: 'pending',
          startRel: 'char:17',
          endRel: 'char:23',
          range: { from: 18, to: 24 },
        } satisfies StoredMark,
      }),
      markId: entityInsertMarkId,
      action: 'accept',
    });
    assert(
      'ok' in entityInsertResult && entityInsertResult.ok === true,
      `Expected rehydration accept to decode projection-space entities, got ${JSON.stringify(entityInsertResult)}`,
    );
    assertEqual(
      stripProofSpanTags(entityInsertResult.markdown).trim(),
      'Alpha beta gamma. delta',
      'Expected projection-space entity decoding to preserve the accepted insertion text',
    );

    const repeatedInsertMarks = canonicalizeStoredMarks({
      [entityInsertMarkId]: {
        kind: 'insert',
        by: 'human:test',
        createdAt,
        quote: 'delta',
        content: ' delta',
        status: 'pending',
        startRel: 'char:17',
        endRel: 'char:23',
        range: { from: 18, to: 24 },
      } satisfies StoredMark,
    });
    const repeatedFirstAccept = await finalizeSuggestionThroughRehydration({
      markdown: 'Alpha beta gamma. delta&#x20;',
      marks: repeatedInsertMarks,
      markId: entityInsertMarkId,
      action: 'accept',
    });
    assert(
      repeatedFirstAccept.ok,
      `Expected first repeated rehydration accept to succeed, got ${JSON.stringify(repeatedFirstAccept)}`,
    );
    const repeatedSecondAccept = await finalizeSuggestionThroughRehydration({
      markdown: 'Alpha beta gamma. delta&#x20;',
      marks: repeatedInsertMarks,
      markId: entityInsertMarkId,
      action: 'accept',
    });
    assert(
      repeatedSecondAccept.ok,
      `Expected second repeated rehydration accept to succeed after tombstone reset, got ${JSON.stringify(repeatedSecondAccept)}`,
    );
    const repeatedRepair = await rehydrateProofMarksMarkdown(
      'Alpha beta gamma. delta&#x20;',
      repeatedInsertMarks,
    );
    assert(
      repeatedRepair.ok,
      `Expected rehydrate-only pass to remain stable after repeated accepts, got ${JSON.stringify(repeatedRepair)}`,
    );

    const longSlug = `rehydrate-add-accepted-${Math.random().toString(36).slice(2, 10)}`;
    const longQuote = `Start ${'a'.repeat(140)} end`;
    db.createDocument(longSlug, `Before ${longQuote} after.`, {}, 'Accepted long quote');

    const addAccepted = await executeDocumentOperationAsync(longSlug, 'POST', '/marks/suggest-replace', {
      quote: longQuote,
      content: 'REPLACED',
      by: 'ai:test',
      status: 'accepted',
    });
    assertEqual(addAccepted.status, 200, `Expected accepted long quote suggestion.add to succeed, got ${addAccepted.status}`);
    const longDoc = db.getDocumentBySlug(longSlug);
    assertEqual(
      stripProofSpanTags(longDoc?.markdown ?? ''),
      'Before REPLACED after.\n',
      'Expected accepted long quote suggestion.add to use structured finalization',
    );

    const inertCommentSlug = `rehydrate-inert-comment-${Math.random().toString(36).slice(2, 10)}`;
    const inertCommentBase = '# Hello there';
    const inertCommentAnchors = buildRelativeAnchors(inertCommentBase, 'Hello');
    const inertCommentSuggestionId = 'rehydrate-inert-suggestion';
    db.createDocument(
      inertCommentSlug,
      inertCommentBase,
      canonicalizeStoredMarks({
        staleComment: {
          kind: 'comment',
          by: 'human:test',
          createdAt,
          quote: 'Hello',
          resolved: false,
          startRel: inertCommentAnchors.startRel,
          endRel: inertCommentAnchors.endRel,
          range: inertCommentAnchors.range,
        } satisfies StoredMark,
        [inertCommentSuggestionId]: {
          kind: 'replace',
          by: 'ai:test',
          createdAt,
          quote: 'Hello',
          content: 'Hi',
          status: 'pending',
          startRel: inertCommentAnchors.startRel,
          endRel: inertCommentAnchors.endRel,
          range: inertCommentAnchors.range,
        } satisfies StoredMark,
      }),
      'Ignore inert comment metadata during accept',
    );

    const inertCommentAccept = await executeDocumentOperationAsync(inertCommentSlug, 'POST', '/marks/accept', {
      markId: inertCommentSuggestionId,
      by: 'human:test',
    });
    assertEqual(inertCommentAccept.status, 200, `Expected accept with inert comment metadata to succeed, got ${inertCommentAccept.status}`);
    const inertCommentDoc = db.getDocumentBySlug(inertCommentSlug);
    assert(
      stripProofSpanTags(inertCommentDoc?.markdown ?? '').includes('Hi there'),
      'Expected accept to succeed even when unrelated incomplete comment metadata exists',
    );

    const nestedSlug = `rehydrate-nested-${Math.random().toString(36).slice(2, 10)}`;
    const nestedMarkId = 'legacy-nested-suggestion';
    const nestedCommentId = 'legacy-nested-comment';
    const nestedQuote = 'Alpha Beta Gamma Delta Epsilon Zeta Eta Theta Iota Kappa Lambda Mu Nu Xi Omicron Pi Rho Sigma Tau';
    const nestedCommentQuote = 'Gamma Delta Epsilon';
    const nestedBase = `Before ${nestedQuote} After`;
    const nestedAnchors = buildRelativeAnchors(nestedBase, nestedQuote);
    const nestedCommentAnchors = buildRelativeAnchors(nestedBase, nestedCommentQuote);
    const nestedMarkdown = `Before <span data-proof="suggestion" data-id="${nestedMarkId}" data-by="ai:test" data-kind="replace">${nestedQuote.slice(0, 48)}</span> After`;
    const nestedMarks = canonicalizeStoredMarks({
      [nestedMarkId]: {
        kind: 'replace',
        by: 'ai:test',
        createdAt,
        quote: nestedQuote,
        content: 'Alpha Beta Rewritten',
        status: 'pending',
        startRel: nestedAnchors.startRel,
        endRel: nestedAnchors.endRel,
        range: nestedAnchors.range,
      } satisfies StoredMark,
      [nestedCommentId]: {
        kind: 'comment',
        by: 'human:test',
        createdAt,
        quote: nestedCommentQuote,
        text: 'Need to revisit this clause',
        threadId: nestedCommentId,
        thread: [],
        replies: [],
        resolved: false,
        startRel: nestedCommentAnchors.startRel,
        endRel: nestedCommentAnchors.endRel,
        range: nestedCommentAnchors.range,
      } satisfies StoredMark,
    });

    const nestedRepair = await rehydrateProofMarksMarkdown(nestedMarkdown, nestedMarks);
    assert(nestedRepair.ok, `Expected nested repair to succeed, got ${nestedRepair.ok ? 'ok' : nestedRepair.error}`);
    const nestedVisibleText = stripAllProofSpanTags(nestedRepair.markdown);
    assert(nestedVisibleText.startsWith('Before'), 'Expected nested repair to preserve surrounding text');
    assert(nestedVisibleText.includes(nestedQuote), 'Expected nested repair to rebuild the full quoted text');
    assert(nestedVisibleText.trim().endsWith('After'), 'Expected nested repair to preserve trailing text');
    assert(nestedRepair.markdown.includes(`data-id="${nestedMarkId}"`), 'Expected nested repair to keep the suggestion wrapper');
    assert(nestedRepair.markdown.includes(`data-id="${nestedCommentId}"`), 'Expected nested repair to restore the nested comment wrapper');
    assertEqual(
      Object.keys(nestedRepair.marks).sort().join(','),
      Object.keys(nestedMarks).sort().join(','),
      'Expected nested repair to preserve mark ids',
    );

    db.createDocument(nestedSlug, nestedMarkdown, nestedMarks, 'Nested repair write');
    const repairReport = await repairProofMarksForSlug(nestedSlug, { write: true });
    assert(repairReport.textStable, 'Expected nested repair to preserve replacement-aware visible text');
    assert(repairReport.safeToWrite, 'Expected nested repair to be safe to write');
    assert(repairReport.wrote, 'Expected nested repair write to persist');
    const repairedNestedDoc = db.getDocumentBySlug(nestedSlug);
    assert(repairedNestedDoc?.markdown.includes(`data-id="${nestedCommentId}"`), 'Expected persisted repair to keep nested comment wrappers');

    const splitQuote = 'Alpha Beta Gamma Delta Epsilon Zeta Eta';
    const splitCommentQuote = 'Gamma Delta Epsilon';
    const splitBase = `Before ${splitQuote} After`;
    const splitAnchors = buildRelativeAnchors(splitBase, splitQuote);
    const splitCommentAnchors = buildRelativeAnchors(splitBase, splitCommentQuote);
    const buildSplitFixture = (suffix: string): {
      suggestionId: string;
      commentId: string;
      markdown: string;
      marks: Record<string, StoredMark>;
    } => {
      const suggestionId = `legacy-split-suggestion-${suffix}`;
      const commentId = `legacy-split-comment-${suffix}`;
      return {
        suggestionId,
        commentId,
        markdown: [
          'Before ',
          `<span data-proof="suggestion" data-id="${suggestionId}" data-by="ai:test" data-kind="replace">Alpha Beta </span>`,
          `<span data-proof="comment" data-id="${commentId}" data-by="human:test">${splitCommentQuote}</span>`,
          `<span data-proof="suggestion" data-id="${suggestionId}" data-by="ai:test" data-kind="replace"> Zeta Eta</span>`,
          ' After',
        ].join(''),
        marks: canonicalizeStoredMarks({
          [suggestionId]: {
            kind: 'replace',
            by: 'ai:test',
            createdAt,
            quote: splitQuote,
            content: splitQuote,
            status: 'pending',
            startRel: splitAnchors.startRel,
            endRel: splitAnchors.endRel,
            range: splitAnchors.range,
          } satisfies StoredMark,
          [commentId]: {
            kind: 'comment',
            by: 'human:test',
            createdAt,
            quote: splitCommentQuote,
            text: 'Nested comment should survive suggestion finalization',
            threadId: commentId,
            thread: [],
            replies: [],
            resolved: false,
            startRel: splitCommentAnchors.startRel,
            endRel: splitCommentAnchors.endRel,
            range: splitCommentAnchors.range,
          } satisfies StoredMark,
        }),
      };
    };

    const splitAcceptFixture = buildSplitFixture('accept');
    const splitSuggestionSlug = `rehydrate-split-accept-${Math.random().toString(36).slice(2, 10)}`;
    db.createDocument(splitSuggestionSlug, splitAcceptFixture.markdown, splitAcceptFixture.marks, 'Split suggestion accept');
    const splitAcceptResult = await executeDocumentOperationAsync(splitSuggestionSlug, 'POST', '/marks/accept', {
      markId: splitAcceptFixture.suggestionId,
      by: 'human:test',
    });
    assertEqual(splitAcceptResult.status, 200, `Expected split suggestion accept to succeed, got ${splitAcceptResult.status}`);
    const splitAcceptedDoc = db.getDocumentBySlug(splitSuggestionSlug);
    assertEqual(
      stripAllProofSpanTags(splitAcceptedDoc?.markdown ?? '').trim(),
      splitBase,
      'Expected split suggestion accept to preserve plain text without duplication',
    );
    assert(
      !splitAcceptedDoc?.markdown.includes(`data-id="${splitAcceptFixture.suggestionId}"`),
      'Expected split suggestion accept to remove all legacy suggestion wrappers',
    );
    assert(
      splitAcceptedDoc?.markdown.includes(`data-id="${splitAcceptFixture.commentId}"`),
      'Expected split suggestion accept to preserve nested comment markup',
    );
    const splitAcceptedMarks = parseStoredMarks(splitAcceptedDoc?.marks);
    assert(splitAcceptFixture.commentId in splitAcceptedMarks, 'Expected split suggestion accept to preserve nested comment metadata');

    const splitRejectFixture = buildSplitFixture('reject');
    const splitRejectSlug = `rehydrate-split-reject-${Math.random().toString(36).slice(2, 10)}`;
    db.createDocument(splitRejectSlug, splitRejectFixture.markdown, splitRejectFixture.marks, 'Split suggestion reject');
    const splitRejectResult = await executeDocumentOperationAsync(splitRejectSlug, 'POST', '/marks/reject', {
      markId: splitRejectFixture.suggestionId,
      by: 'human:test',
    });
    assertEqual(splitRejectResult.status, 200, `Expected split suggestion reject to succeed, got ${splitRejectResult.status}`);
    const splitRejectedDoc = db.getDocumentBySlug(splitRejectSlug);
    assertEqual(
      stripAllProofSpanTags(splitRejectedDoc?.markdown ?? '').trim(),
      splitBase,
      'Expected split suggestion reject to preserve plain text without duplication',
    );
    assert(
      !splitRejectedDoc?.markdown.includes(`data-id="${splitRejectFixture.suggestionId}"`),
      'Expected split suggestion reject to remove all legacy suggestion wrappers',
    );
    assert(
      splitRejectedDoc?.markdown.includes(`data-id="${splitRejectFixture.commentId}"`),
      'Expected split suggestion reject to preserve nested comment markup',
    );
    const splitRejectedMarks = parseStoredMarks(splitRejectedDoc?.marks);
    assert(splitRejectFixture.commentId in splitRejectedMarks, 'Expected split suggestion reject to preserve nested comment metadata');

    const splitRepairFixture = buildSplitFixture('repair');
    const splitRepair = await rehydrateProofMarksMarkdown(splitRepairFixture.markdown, splitRepairFixture.marks);
    assert(splitRepair.ok, `Expected split repair to succeed, got ${splitRepair.ok ? 'ok' : splitRepair.error}`);
    assertEqual(
      stripAllProofSpanTags(splitRepair.markdown).trim(),
      splitBase,
      'Expected split repair to preserve plain text without duplication',
    );
    assert(
      splitRepair.markdown.includes(`data-id="${splitRepairFixture.suggestionId}"`),
      'Expected split repair to restore the split suggestion wrapper',
    );
    assert(
      splitRepair.markdown.includes(`data-id="${splitRepairFixture.commentId}"`),
      'Expected split repair to restore the nested comment wrapper',
    );
    assertEqual(
      Object.keys(splitRepair.marks).sort().join(','),
      Object.keys(splitRepairFixture.marks).sort().join(','),
      'Expected split repair to preserve mark ids',
    );

    const splitRepairSlug = `rehydrate-split-repair-${Math.random().toString(36).slice(2, 10)}`;
    db.createDocument(splitRepairSlug, splitRepairFixture.markdown, splitRepairFixture.marks, 'Split suggestion repair write');
    const splitRepairReport = await repairProofMarksForSlug(splitRepairSlug, { write: true });
    assert(splitRepairReport.textStable, 'Expected split repair write to preserve visible text');
    assert(splitRepairReport.safeToWrite, 'Expected split repair write to be safe');
    assert(splitRepairReport.wrote, 'Expected split repair write to persist');
    const repairedSplitDoc = db.getDocumentBySlug(splitRepairSlug);
    assertEqual(
      stripAllProofSpanTags(repairedSplitDoc?.markdown ?? '').trim(),
      splitBase,
      'Expected persisted split repair to preserve plain text without duplication',
    );
    assert(
      repairedSplitDoc?.markdown.includes(`data-id="${splitRepairFixture.commentId}"`),
      'Expected persisted split repair to keep the nested comment wrapper',
    );

    const staleAuthoredSlug = `rehydrate-stale-authored-${Math.random().toString(36).slice(2, 10)}`;
    const staleAuthoredSuggestionId = 'stale-authored-suggestion';
    const staleAuthoredBase = 'Before Hello After';
    const staleAuthoredAnchors = buildRelativeAnchors(staleAuthoredBase, 'Hello');
    db.createDocument(
      staleAuthoredSlug,
      'Before <span data-proof="suggestion" data-id="stale-authored-suggestion" data-by="ai:test" data-kind="replace">Hell</span> After',
      canonicalizeStoredMarks({
        [staleAuthoredSuggestionId]: {
          kind: 'replace',
          by: 'ai:test',
          createdAt,
          quote: 'Hello',
          content: 'Hi',
          status: 'pending',
          startRel: staleAuthoredAnchors.startRel,
          endRel: staleAuthoredAnchors.endRel,
          range: staleAuthoredAnchors.range,
        } satisfies StoredMark,
        'authored:human:R2C2 Repro:90-92': {
          kind: 'authored',
          by: 'human:R2C2 Repro',
          createdAt: '1970-01-01T00:00:00.000Z',
          quote: 'XY',
          startRel: 'char:87',
          endRel: 'char:89',
          range: { from: 90, to: 92 },
        } satisfies StoredMark,
        'authored:human:R2C2 Repro:92-94': {
          kind: 'authored',
          by: 'human:R2C2 Repro',
          createdAt: '1970-01-01T00:00:00.000Z',
          quote: 'XY',
          startRel: 'char:89',
          endRel: 'char:91',
          range: { from: 92, to: 94 },
        } satisfies StoredMark,
      }),
      'Ignore stale orphaned authored metadata during accept',
    );

    const staleAuthoredAccept = await executeDocumentOperationAsync(staleAuthoredSlug, 'POST', '/marks/accept', {
      markId: staleAuthoredSuggestionId,
      by: 'human:test',
    });
    assertEqual(staleAuthoredAccept.status, 200, `Expected accept with stale orphaned authored metadata to succeed, got ${staleAuthoredAccept.status}`);
    const staleAuthoredDoc = db.getDocumentBySlug(staleAuthoredSlug);
    assert(
      stripAllProofSpanTags(staleAuthoredDoc?.markdown ?? '').includes('Before Hi After'),
      'Expected stale orphaned authored metadata to be ignored when it is not serialized in markdown',
    );

    const missingAuthoredSlug = `rehydrate-missing-authored-${Math.random().toString(36).slice(2, 10)}`;
    const missingAuthoredSuggestionId = 'missing-authored-suggestion';
    const missingAuthoredVisibleText = 'Visible authored provenance';
    const missingAuthoredQuote = 'Editable legacy quote';
    const missingAuthoredReplacement = 'Accepted replacement text';
    const missingAuthoredBase = `${missingAuthoredVisibleText} ${missingAuthoredQuote}`;
    const missingAuthoredAnchors = buildRelativeAnchors(missingAuthoredBase, missingAuthoredQuote);
    db.createDocument(
      missingAuthoredSlug,
      [
        `<span data-proof="authored" data-by="human:dan">${missingAuthoredVisibleText}</span> `,
        `<span data-proof="suggestion" data-id="${missingAuthoredSuggestionId}" data-by="ai:test" data-kind="replace">${missingAuthoredQuote.slice(0, 10)}</span>`,
      ].join(''),
      canonicalizeStoredMarks({
        [missingAuthoredSuggestionId]: {
          kind: 'replace',
          by: 'ai:test',
          createdAt,
          quote: missingAuthoredQuote,
          content: missingAuthoredReplacement,
          status: 'pending',
          startRel: missingAuthoredAnchors.startRel,
          endRel: missingAuthoredAnchors.endRel,
          range: missingAuthoredAnchors.range,
        } satisfies StoredMark,
      }),
      'Fail when visible authored spans are missing stored metadata',
    );

    const missingAuthoredAccept = await executeDocumentOperationAsync(missingAuthoredSlug, 'POST', '/marks/accept', {
      markId: missingAuthoredSuggestionId,
      by: 'human:test',
    });
    assertEqual(
      missingAuthoredAccept.status,
      200,
      `Expected accept with missing visible authored metadata to recover authored spans, got ${missingAuthoredAccept.status}`,
    );
    const missingAuthoredDoc = db.getDocumentBySlug(missingAuthoredSlug);
    assert(
      (missingAuthoredDoc?.markdown ?? '').includes('data-proof="authored"'),
      'Expected accept to preserve visible authored spans even when stored authored metadata was missing',
    );
    const missingAuthoredMarks = parseStoredMarks(missingAuthoredDoc?.marks ?? '');
    assert(
      Object.values(missingAuthoredMarks).some((mark) => mark.kind === 'authored'),
      'Expected accept to backfill authored metadata from serialized markdown during structured rehydration',
    );

    const authoredGapSlug = `authored-gap-${Math.random().toString(36).slice(2, 10)}`;
    const authoredGapQuote = 'beta';
    const authoredGapBase = 'Alpha beta gamma';
    const authoredGapAnchors = buildRelativeAnchors(authoredGapBase, authoredGapQuote);
    const authoredGapSuggestionId = 'authored-gap-suggestion';
    const authoredGapAuthoredId = 'authored-gap-authored';
    db.createDocument(
      authoredGapSlug,
      `Alpha <span data-proof="suggestion" data-id="${authoredGapSuggestionId}" data-by="ai:test" data-kind="delete">${authoredGapQuote}</span> gamma`,
      canonicalizeStoredMarks({
        [authoredGapSuggestionId]: {
          kind: 'delete',
          by: 'ai:test',
          createdAt,
          quote: authoredGapQuote,
          status: 'pending',
          startRel: authoredGapAnchors.startRel,
          endRel: authoredGapAnchors.endRel,
          range: authoredGapAnchors.range,
        } satisfies StoredMark,
        [authoredGapAuthoredId]: {
          kind: 'authored',
          by: 'human:dan',
          createdAt,
          quote: 'ghost authored text',
          startRel: 'char:999',
          endRel: 'char:1017',
          range: { from: 999, to: 1017 },
        } satisfies StoredMark,
      }),
      'Accept should ignore unrelated authored hydration gaps',
    );

    const authoredGapAccept = await executeDocumentOperationAsync(authoredGapSlug, 'POST', '/marks/accept', {
      markId: authoredGapSuggestionId,
      by: 'human:test',
    });
    assertEqual(
      authoredGapAccept.status,
      200,
      `Expected accept to ignore unrelated authored hydration gaps, got ${authoredGapAccept.status}`,
    );
    const authoredGapDoc = db.getDocumentBySlug(authoredGapSlug);
    assert(
      !(authoredGapDoc?.markdown ?? '').includes('data-proof="suggestion"'),
      'Expected authored hydration gaps not to block removing the accepted suggestion wrapper',
    );

    const unrelatedSuggestionGapSlug = `suggestion-gap-${Math.random().toString(36).slice(2, 10)}`;
    const unrelatedSuggestionGapQuote = 'beta';
    const unrelatedSuggestionGapBase = 'Alpha beta gamma';
    const unrelatedSuggestionGapAnchors = buildRelativeAnchors(unrelatedSuggestionGapBase, unrelatedSuggestionGapQuote);
    const unrelatedSuggestionGapSuggestionId = 'suggestion-gap-target';
    const unrelatedPendingSuggestionId = 'suggestion-gap-missing-insert';
    db.createDocument(
      unrelatedSuggestionGapSlug,
      `Alpha <span data-proof="suggestion" data-id="${unrelatedSuggestionGapSuggestionId}" data-by="ai:test" data-kind="delete">${unrelatedSuggestionGapQuote}</span> gamma`,
      canonicalizeStoredMarks({
        [unrelatedSuggestionGapSuggestionId]: {
          kind: 'delete',
          by: 'ai:test',
          createdAt,
          quote: unrelatedSuggestionGapQuote,
          status: 'pending',
          startRel: unrelatedSuggestionGapAnchors.startRel,
          endRel: unrelatedSuggestionGapAnchors.endRel,
          range: unrelatedSuggestionGapAnchors.range,
        } satisfies StoredMark,
        [unrelatedPendingSuggestionId]: {
          kind: 'insert',
          by: 'human:dan',
          createdAt,
          quote: 'Detached pending insertion',
          content: 'Detached pending insertion',
          status: 'pending',
          startRel: 'char:999',
          endRel: 'char:1024',
          range: { from: 999, to: 1024 },
        } satisfies StoredMark,
      }),
      'Accept should preserve unrelated pending suggestion hydration gaps',
    );

    const unrelatedSuggestionGapAccept = await executeDocumentOperationAsync(unrelatedSuggestionGapSlug, 'POST', '/marks/accept', {
      markId: unrelatedSuggestionGapSuggestionId,
      by: 'human:test',
    });
    assertEqual(
      unrelatedSuggestionGapAccept.status,
      200,
      `Expected accept to preserve unrelated pending suggestion hydration gaps, got ${unrelatedSuggestionGapAccept.status}`,
    );
    const unrelatedSuggestionGapDoc = db.getDocumentBySlug(unrelatedSuggestionGapSlug);
    assertEqual(
      stripAllProofSpanTags(unrelatedSuggestionGapDoc?.markdown ?? '').replace(/\s+/g, ' ').trim(),
      'Alpha gamma',
      'Expected unrelated pending suggestion hydration gaps not to block the accepted deletion',
    );
    const unrelatedSuggestionGapMarks = parseStoredMarks(unrelatedSuggestionGapDoc?.marks ?? '');
    assert(
      unrelatedPendingSuggestionId in unrelatedSuggestionGapMarks,
      'Expected accept to preserve unrelated pending suggestion metadata even when it could not be rehydrated',
    );
    assertEqual(
      unrelatedSuggestionGapMarks[unrelatedPendingSuggestionId]?.status,
      'pending',
      'Expected preserved unrelated suggestion metadata to remain pending after accepting a different suggestion',
    );

    const duplicateInsertSlug = `rehydrate-duplicate-insert-${Math.random().toString(36).slice(2, 10)}`;
    const duplicateInsertMarkId = 'duplicate-insert-accept';
    const duplicateInsertQuote = 'reveals shortcuts. It woudlf be nice';
    const duplicateInsertBase = `We should have something in the menu bar that ${duplicateInsertQuote} ${duplicateInsertQuote}`;
    const duplicateInsertAnchors = buildRelativeAnchors(duplicateInsertBase, duplicateInsertQuote);
    db.createDocument(
      duplicateInsertSlug,
      `We should have something in the menu bar that <span data-proof="suggestion" data-id="${duplicateInsertMarkId}" data-by="human:test" data-kind="insert">${duplicateInsertQuote}</span> ${duplicateInsertQuote}`,
      canonicalizeStoredMarks({
        [duplicateInsertMarkId]: {
          kind: 'insert',
          by: 'human:test',
          createdAt,
          quote: duplicateInsertQuote,
          content: duplicateInsertQuote,
          status: 'pending',
          startRel: `char:${Number.parseInt(duplicateInsertAnchors.startRel.slice(5), 10) + 1}`,
          endRel: `char:${Number.parseInt(duplicateInsertAnchors.endRel.slice(5), 10) + 1}`,
          range: {
            from: duplicateInsertAnchors.range.from + 1,
            to: duplicateInsertAnchors.range.to + 1,
          },
        } satisfies StoredMark,
      }),
      'Duplicate insertion accept should recover nearby anchors',
    );

    const duplicateInsertAccept = await executeDocumentOperationAsync(duplicateInsertSlug, 'POST', '/marks/accept', {
      markId: duplicateInsertMarkId,
      by: 'human:test',
    });
    assertEqual(
      duplicateInsertAccept.status,
      200,
      `Expected duplicate insertion accept to recover nearby anchors, got ${duplicateInsertAccept.status}`,
    );
    const duplicateInsertDoc = db.getDocumentBySlug(duplicateInsertSlug);
    assert(
      !(duplicateInsertDoc?.markdown ?? '').includes('data-proof="suggestion"'),
      'Expected duplicate insertion accept to remove the suggestion wrapper after recovering the nearby quote anchor',
    );
    const duplicateInsertText = stripAllProofSpanTags(duplicateInsertDoc?.markdown ?? '');
    assertEqual(
      duplicateInsertText.split(duplicateInsertQuote).length - 1,
      2,
      'Expected duplicate insertion accept to preserve both visible copies of the repeated insertion text',
    );
    const duplicateInsertMarks = parseStoredMarks(duplicateInsertDoc?.marks ?? '');
    assert(
      !(duplicateInsertMarkId in duplicateInsertMarks),
      'Expected duplicate insertion accept to clear the accepted insertion metadata',
    );

    const authoredBeforeText = 'Lead authored text.';
    const authoredAfterText = 'Tail authored text.';
    const authoredQuote = 'Middle legacy quote that should stay anchored.';
    const authoredReplacement = 'Middle accepted text that still preserves provenance.';
    const authoredBase = `${authoredBeforeText} ${authoredQuote} ${authoredAfterText}`;
    const authoredSuggestionAnchors = buildRelativeAnchors(authoredBase, authoredQuote);
    const authoredBeforeAnchors = buildRelativeAnchors(authoredBase, authoredBeforeText);
    const authoredAfterAnchors = buildRelativeAnchors(authoredBase, authoredAfterText);
    const buildAuthoredFixture = (suffix: string): {
      beforeId: string;
      afterId: string;
      suggestionId: string;
      markdown: string;
      marks: Record<string, StoredMark>;
    } => {
      const beforeId = `authored-before-${suffix}`;
      const afterId = `authored-after-${suffix}`;
      const suggestionId = `authored-suggestion-${suffix}`;
      return {
        beforeId,
        afterId,
        suggestionId,
        markdown: [
          `<span data-proof="authored" data-by="human:dan">${authoredBeforeText}</span> `,
          `<span data-proof="suggestion" data-id="${suggestionId}" data-by="ai:test" data-kind="replace">${authoredQuote.slice(0, 30)}</span> `,
          `<span data-proof="authored" data-by="human:dan">${authoredAfterText}</span>`,
        ].join(''),
        marks: canonicalizeStoredMarks({
          [beforeId]: {
            kind: 'authored',
            by: 'human:dan',
            createdAt,
            quote: authoredBeforeText,
            startRel: authoredBeforeAnchors.startRel,
            endRel: authoredBeforeAnchors.endRel,
            range: authoredBeforeAnchors.range,
          } satisfies StoredMark,
          [afterId]: {
            kind: 'authored',
            by: 'human:dan',
            createdAt,
            quote: authoredAfterText,
            startRel: authoredAfterAnchors.startRel,
            endRel: authoredAfterAnchors.endRel,
            range: authoredAfterAnchors.range,
          } satisfies StoredMark,
          [suggestionId]: {
            kind: 'replace',
            by: 'ai:test',
            createdAt,
            quote: authoredQuote,
            content: authoredReplacement,
            status: 'pending',
            startRel: authoredSuggestionAnchors.startRel,
            endRel: authoredSuggestionAnchors.endRel,
            range: authoredSuggestionAnchors.range,
          } satisfies StoredMark,
        }),
      };
    };

    const authoredAcceptFixture = buildAuthoredFixture('accept');
    const authoredAcceptSlug = `rehydrate-authored-accept-${Math.random().toString(36).slice(2, 10)}`;
    db.createDocument(authoredAcceptSlug, authoredAcceptFixture.markdown, authoredAcceptFixture.marks, 'Authored preservation accept');
    const authoredAcceptResult = await executeDocumentOperationAsync(authoredAcceptSlug, 'POST', '/marks/accept', {
      markId: authoredAcceptFixture.suggestionId,
      by: 'human:test',
    });
    assertEqual(authoredAcceptResult.status, 200, `Expected authored preservation accept to succeed, got ${authoredAcceptResult.status}`);
    const authoredAcceptedDoc = db.getDocumentBySlug(authoredAcceptSlug);
    assertEqual(
      stripAllProofSpanTags(authoredAcceptedDoc?.markdown ?? '').trim(),
      `${authoredBeforeText} ${authoredReplacement} ${authoredAfterText}`,
      'Expected authored preservation accept to apply the replacement without dropping authored spans',
    );
    assertEqual(
      (authoredAcceptedDoc?.markdown.match(/data-proof="authored"[^>]*data-by="human:dan"/g) ?? []).length,
      2,
      'Expected authored preservation accept to keep both original human authored wrappers',
    );
    const authoredAcceptedMarks = parseStoredMarks(authoredAcceptedDoc?.marks);
    assert(authoredAcceptFixture.beforeId in authoredAcceptedMarks, 'Expected authored preservation accept to keep the leading authored mark id');
    assert(authoredAcceptFixture.afterId in authoredAcceptedMarks, 'Expected authored preservation accept to keep the trailing authored mark id');

    const authoredRejectFixture = buildAuthoredFixture('reject');
    const authoredRejectSlug = `rehydrate-authored-reject-${Math.random().toString(36).slice(2, 10)}`;
    db.createDocument(authoredRejectSlug, authoredRejectFixture.markdown, authoredRejectFixture.marks, 'Authored preservation reject');
    const authoredRejectResult = await executeDocumentOperationAsync(authoredRejectSlug, 'POST', '/marks/reject', {
      markId: authoredRejectFixture.suggestionId,
      by: 'human:test',
    });
    assertEqual(authoredRejectResult.status, 200, `Expected authored preservation reject to succeed, got ${authoredRejectResult.status}`);
    const authoredRejectedDoc = db.getDocumentBySlug(authoredRejectSlug);
    assertEqual(
      stripAllProofSpanTags(authoredRejectedDoc?.markdown ?? '').trim(),
      authoredBase,
      'Expected authored preservation reject to keep the original text without dropping authored spans',
    );
    assertEqual(
      (authoredRejectedDoc?.markdown.match(/data-proof="authored"[^>]*data-by="human:dan"/g) ?? []).length,
      2,
      'Expected authored preservation reject to keep both original human authored wrappers',
    );
    const authoredRejectedMarks = parseStoredMarks(authoredRejectedDoc?.marks);
    assert(authoredRejectFixture.beforeId in authoredRejectedMarks, 'Expected authored preservation reject to keep the leading authored mark id');
    assert(authoredRejectFixture.afterId in authoredRejectedMarks, 'Expected authored preservation reject to keep the trailing authored mark id');

    console.log('✓ proof mark rehydration repairs legacy accept/reject/add-accepted and nested repair flows');
  } finally {
    try {
      unlinkSync(dbPath);
    } catch {
      // Ignore cleanup failures for temp DBs.
    }

    process.env.DATABASE_PATH = prevDatabasePath;
    process.env.PROOF_ENV = prevProofEnv;
    process.env.NODE_ENV = prevNodeEnv;
    if (prevDbEnvInit === undefined) {
      delete process.env.PROOF_DB_ENV_INIT;
    } else {
      process.env.PROOF_DB_ENV_INIT = prevDbEnvInit;
    }
  }
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
