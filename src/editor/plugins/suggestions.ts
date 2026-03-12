/**
 * Suggestions Plugin for Milkdown
 *
 * Converts edits into proofSuggestion marks + PROOF metadata
 * when suggestions mode is enabled.
 */

import { $ctx, $prose } from '@milkdown/kit/utils';
import { Plugin, PluginKey, TextSelection, type EditorState, type Transaction } from '@milkdown/kit/prose/state';
import type { MarkType, Node as ProseMirrorNode } from '@milkdown/kit/prose/model';

import { marksPluginKey, getMarkMetadata, buildSuggestionMetadata, getMarks } from './marks';
import { generateMarkId, normalizeQuote, type InsertData, type MarkRange, type StoredMark } from '../../formats/marks';
import { getCurrentActor } from '../actor';

// Suggestion state
export interface SuggestionState {
  enabled: boolean;
}

// Plugin key for accessing state
export const suggestionsPluginKey = new PluginKey<SuggestionState>('suggestions');

// Context to store suggestion state
export const suggestionsCtx = $ctx<SuggestionState, 'suggestions'>({ enabled: false }, 'suggestions');

type SuggestionKind = 'insert' | 'delete' | 'replace';

type SliceNode = {
  type?: string;
  text?: string;
  content?: SliceNode[];
};

const COALESCE_WINDOW_MS = 750;

type InsertCoalesceState = { id: string; from: number; to: number; by: string; updatedAt: number };

type EditableSuggestionCandidate = {
  id: string;
  kind: 'insert' | 'replace';
  range: MarkRange;
  originalQuote?: string;
};

const lastInsertByActor = new Map<string, InsertCoalesceState>();

function normalizeSuggestionKind(kind: unknown): SuggestionKind {
  if (kind === 'insert' || kind === 'delete' || kind === 'replace') return kind;
  return 'replace';
}

function isWhitespaceOnly(text: string): boolean {
  return /^[\s\u00A0]+$/.test(text);
}

function getCoalescableInsertCandidate(
  state: EditorState,
  pos: number,
  by: string,
  now: number
): { id: string; range: MarkRange; direction: 'append' | 'prepend' } | null {
  const cached = lastInsertByActor.get(by);
  if (!cached) return null;
  if (now - cached.updatedAt > COALESCE_WINDOW_MS) {
    lastInsertByActor.delete(by);
    return null;
  }

  const marks = getMarks(state);
  const match = marks.find(mark => mark.id === cached.id && mark.kind === 'insert' && mark.by === by);
  if (!match || !match.range) {
    lastInsertByActor.delete(by);
    return null;
  }

  const data = match.data as InsertData | undefined;
  if (data?.status && data.status !== 'pending') {
    lastInsertByActor.delete(by);
    return null;
  }

  if (match.range.to === pos) {
    return { id: match.id, range: match.range, direction: 'append' };
  }

  if (match.range.from === pos) {
    return { id: match.id, range: match.range, direction: 'prepend' };
  }

  return null;
}

function collectSliceText(nodes?: SliceNode[]): { text: string; hasNonText: boolean } {
  let text = '';
  let hasNonText = false;

  if (!nodes) return { text, hasNonText };

  for (const node of nodes) {
    if (node.text) {
      text += node.text;
    }
    if (node.type && node.type !== 'text') {
      hasNonText = true;
    }
    if (node.content) {
      const child = collectSliceText(node.content);
      text += child.text;
      if (child.hasNonText) hasNonText = true;
    }
  }

  return { text, hasNonText };
}

function detectSuggestionKinds(
  doc: ProseMirrorNode,
  from: number,
  to: number,
  suggestionType: MarkType
): { hasInsert: boolean; hasDelete: boolean; hasReplace: boolean } {
  const found = { hasInsert: false, hasDelete: false, hasReplace: false };

  doc.nodesBetween(from, to, (node) => {
    if (!node.isText) return true;
    for (const mark of node.marks) {
      if (mark.type !== suggestionType) continue;
      const kind = normalizeSuggestionKind(mark.attrs.kind);
      if (kind === 'insert') found.hasInsert = true;
      if (kind === 'delete') found.hasDelete = true;
      if (kind === 'replace') found.hasReplace = true;
    }
    return !(found.hasInsert && found.hasDelete && found.hasReplace);
  });

  return found;
}

function getSuggestionAnchorRange(
  doc: ProseMirrorNode,
  suggestionType: MarkType,
  id: string
): MarkRange | null {
  let range: MarkRange | null = null;

  doc.descendants((node, pos) => {
    if (!node.isText) return true;
    const matches = node.marks.some((mark) => mark.type === suggestionType && mark.attrs.id === id);
    if (!matches) return true;

    const from = pos;
    const to = pos + node.nodeSize;
    if (range && from <= range.to) {
      range.to = Math.max(range.to, to);
    } else if (!range) {
      range = { from, to };
    }
    return true;
  });

  return range;
}

function findEditableSuggestionCandidate(
  doc: ProseMirrorNode,
  metadata: Record<string, StoredMark>,
  suggestionType: MarkType,
  from: number,
  to: number,
  actor: string
): EditableSuggestionCandidate | null {
  const candidates = new Map<string, EditableSuggestionCandidate>();

  doc.descendants((node, pos) => {
    if (!node.isText) return true;
    for (const mark of node.marks) {
      if (mark.type !== suggestionType) continue;
      const id = typeof mark.attrs.id === 'string' ? mark.attrs.id : null;
      if (!id) continue;
      const kind = normalizeSuggestionKind(mark.attrs.kind);
      if (kind !== 'insert' && kind !== 'replace') continue;

      const stored = metadata[id];
      const by = typeof stored?.by === 'string' ? stored.by : String(mark.attrs.by ?? 'unknown');
      const status = stored?.status;
      if (by !== actor || (status && status !== 'pending')) continue;

      const originalQuote = kind === 'replace' && typeof stored?.originalQuote === 'string' && stored.originalQuote.trim().length > 0
        ? stored.originalQuote
        : undefined;
      if (kind === 'replace' && !originalQuote) continue;

      const existing = candidates.get(id);
      const nodeFrom = pos;
      const nodeTo = pos + node.nodeSize;
      if (existing) {
        existing.range.from = Math.min(existing.range.from, nodeFrom);
        existing.range.to = Math.max(existing.range.to, nodeTo);
      } else {
        candidates.set(id, {
          id,
          kind,
          range: { from: nodeFrom, to: nodeTo },
          ...(originalQuote ? { originalQuote } : {}),
        });
      }
    }
    return true;
  });

  const matches = [...candidates.values()].filter((candidate) => {
    if (from === to) {
      return from >= candidate.range.from && from <= candidate.range.to;
    }
    return from >= candidate.range.from && to <= candidate.range.to;
  });

  return matches.length === 1 ? matches[0] : null;
}

function syncEditableSuggestionMetadata(
  metadata: Record<string, StoredMark>,
  doc: ProseMirrorNode,
  suggestionType: MarkType,
  candidate: EditableSuggestionCandidate
): { metadata: Record<string, StoredMark>; range: MarkRange | null } {
  const range = getSuggestionAnchorRange(doc, suggestionType, candidate.id);
  if (!range) {
    if (candidate.kind === 'insert') {
      const next = { ...metadata };
      delete next[candidate.id];
      return { metadata: next, range: null };
    }
    return { metadata, range: null };
  }

  const text = doc.textBetween(range.from, range.to, '\n', '\n');
  const existing = metadata[candidate.id];
  const nextEntry: StoredMark = {
    ...existing,
    kind: candidate.kind,
    content: text,
    quote: normalizeQuote(text),
    range: { from: range.from, to: range.to },
    startRel: undefined,
    endRel: undefined,
    status: existing?.status ?? 'pending',
  };

  if (candidate.originalQuote) {
    nextEntry.originalQuote = candidate.originalQuote;
  }

  return {
    metadata: {
      ...metadata,
      [candidate.id]: nextEntry,
    },
    range,
  };
}

/**
 * Wrap a transaction to convert edits to suggestions when enabled.
 * This intercepts the transaction and converts direct edits into tracked changes:
 * - Insertions get marked with proofSuggestion kind=insert
 * - Deletions get marked with proofSuggestion kind=delete instead of being removed
 * - Replacements can stay anchored on live replacement text while retaining original text in metadata
 */
export function wrapTransactionForSuggestions(
  tr: Transaction,
  state: EditorState,
  enabled: boolean
): Transaction {
  if (!enabled || !tr.docChanged) {
    return tr;
  }
  if (tr.getMeta('y-sync$')) {
    return tr;
  }

  const suggestionType = state.schema.marks.proofSuggestion;

  if (!suggestionType) {
    console.warn('[suggestions] Missing proofSuggestion mark type');
    return tr;
  }

  // Check for structural changes (paragraph splits, etc). Pass through unchanged.
  for (const step of tr.steps) {
    const stepJson = step.toJSON() as { stepType?: string; slice?: { content?: SliceNode[] } };
    if (stepJson.stepType === 'replace' && stepJson.slice?.content) {
      const { hasNonText } = collectSliceText(stepJson.slice.content);
      if (hasNonText) {
        return tr;
      }
    }
  }

  const actor = getCurrentActor();
  let metadata = getMarkMetadata(state);
  let metadataChanged = false;

  // Build a new transaction that converts edits to tracked changes.
  const newTr = state.tr;
  let writeOffset = 0;

  for (const step of tr.steps) {
    const stepJson = step.toJSON() as {
      stepType?: string;
      from?: number;
      to?: number;
      slice?: { content?: SliceNode[] };
    };

    if (stepJson.stepType === 'replace') {
      const origFrom = stepJson.from ?? 0;
      const origTo = stepJson.to ?? 0;
      const from = origFrom + writeOffset;
      const to = origTo + writeOffset;
      const slice = stepJson.slice;

      const { text: insertedText } = collectSliceText(slice?.content);
      const deletedText = state.doc.textBetween(origFrom, origTo, '');

      const docSize = newTr.doc.content.size;
      const safeFrom = Math.max(0, Math.min(from, docSize));
      const safeTo = Math.max(safeFrom, Math.min(to, docSize));
      const editableSuggestion = findEditableSuggestionCandidate(
        newTr.doc,
        metadata,
        suggestionType,
        safeFrom,
        safeTo,
        actor,
      );

      if (editableSuggestion) {
        lastInsertByActor.delete(actor);

        if (!deletedText && insertedText) {
          newTr.insertText(insertedText, safeFrom);
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: editableSuggestion.id, kind: editableSuggestion.kind, by: actor })
          );
          writeOffset += insertedText.length;

          const synced = syncEditableSuggestionMetadata(metadata, newTr.doc, suggestionType, editableSuggestion);
          metadata = synced.metadata;
          metadataChanged = true;

          if (editableSuggestion.kind === 'insert' && synced.range) {
            lastInsertByActor.set(actor, {
              id: editableSuggestion.id,
              from: synced.range.from,
              to: synced.range.to,
              by: actor,
              updatedAt: Date.now(),
            });
          }
          continue;
        }

        if (deletedText && !insertedText) {
          const deletingWholeSuggestion = safeFrom === editableSuggestion.range.from && safeTo === editableSuggestion.range.to;
          if (deletingWholeSuggestion && editableSuggestion.kind === 'replace' && editableSuggestion.originalQuote) {
            newTr.insertText(editableSuggestion.originalQuote, safeFrom, safeTo);
            newTr.addMark(
              safeFrom,
              safeFrom + editableSuggestion.originalQuote.length,
              suggestionType.create({ id: editableSuggestion.id, kind: 'delete', by: actor })
            );
            writeOffset += editableSuggestion.originalQuote.length - deletedText.length;

            metadata = {
              ...metadata,
              [editableSuggestion.id]: {
                ...metadata[editableSuggestion.id],
                kind: 'delete',
                status: metadata[editableSuggestion.id]?.status ?? 'pending',
                quote: normalizeQuote(editableSuggestion.originalQuote),
                content: undefined,
                originalQuote: undefined,
                range: { from: safeFrom, to: safeFrom + editableSuggestion.originalQuote.length },
                startRel: undefined,
                endRel: undefined,
              },
            };
            metadataChanged = true;
            newTr.setSelection(TextSelection.create(newTr.doc, safeFrom));
            continue;
          }

          newTr.delete(safeFrom, safeTo);
          writeOffset -= deletedText.length;

          if (deletingWholeSuggestion && editableSuggestion.kind === 'insert') {
            const nextMetadata = { ...metadata };
            delete nextMetadata[editableSuggestion.id];
            metadata = nextMetadata;
          } else {
            const synced = syncEditableSuggestionMetadata(metadata, newTr.doc, suggestionType, editableSuggestion);
            metadata = synced.metadata;
          }
          metadataChanged = true;
          continue;
        }

        if (deletedText && insertedText) {
          newTr.insertText(insertedText, safeFrom, safeTo);
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: editableSuggestion.id, kind: editableSuggestion.kind, by: actor })
          );
          writeOffset += insertedText.length - deletedText.length;

          const synced = syncEditableSuggestionMetadata(metadata, newTr.doc, suggestionType, editableSuggestion);
          metadata = synced.metadata;
          metadataChanged = true;

          if (editableSuggestion.kind === 'insert' && synced.range) {
            lastInsertByActor.set(actor, {
              id: editableSuggestion.id,
              from: synced.range.from,
              to: synced.range.to,
              by: actor,
              updatedAt: Date.now(),
            });
          }
          continue;
        }
      }

      // CASE 1: Pure deletion (no insertion)
      if (deletedText && !insertedText) {
        lastInsertByActor.delete(actor);
        const existing = detectSuggestionKinds(newTr.doc, safeFrom, safeTo, suggestionType);

        if (existing.hasDelete || existing.hasInsert) {
          // Already tracked: accept deletion or reject insertion
          newTr.delete(safeFrom, safeTo);
          writeOffset -= deletedText.length;
        } else if (existing.hasReplace) {
          // Remove replace suggestion and keep content
          newTr.removeMark(safeFrom, safeTo, suggestionType);
        } else {
          const suggestionId = generateMarkId();
          const createdAt = new Date().toISOString();

          newTr.addMark(safeFrom, safeTo, suggestionType.create({
            id: suggestionId,
            kind: 'delete',
            by: actor,
          }));

          metadata = {
            ...metadata,
            [suggestionId]: buildSuggestionMetadata('delete', actor, null, createdAt),
          };
          metadataChanged = true;

          // Move cursor to start of deletion (don't leave it inside deleted text)
          newTr.setSelection(TextSelection.create(newTr.doc, safeFrom));
        }
      }
      // CASE 2: Pure insertion (no deletion)
      else if (insertedText && !deletedText) {
        const now = Date.now();
        const whitespaceOnly = isWhitespaceOnly(insertedText);
        const candidate = getCoalescableInsertCandidate(state, safeFrom, actor, now);

        if (candidate && whitespaceOnly) {
          // Whitespace with active candidate: extend the mark to include it.
          // This keeps "Proof is" as one suggestion instead of splitting at the space.
          const existingMeta = metadata[candidate.id];
          const existingContent = typeof existingMeta?.content === 'string' ? existingMeta.content : '';
          const updatedContent = candidate.direction === 'append'
            ? `${existingContent}${insertedText}`
            : `${insertedText}${existingContent}`;

          newTr.insertText(insertedText, safeFrom);
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: candidate.id, kind: 'insert', by: actor })
          );
          writeOffset += insertedText.length;

          metadata = {
            ...metadata,
            [candidate.id]: {
              ...existingMeta,
              content: updatedContent,
            },
          };
          metadataChanged = true;

          lastInsertByActor.set(actor, {
            id: candidate.id,
            from: candidate.range.from,
            to: candidate.range.to + insertedText.length,
            by: actor,
            updatedAt: now,
          });
        } else if (candidate) {
          // Non-whitespace with active candidate: coalesce into existing mark
          const existingMeta = metadata[candidate.id];
          const existingContent = typeof existingMeta?.content === 'string' ? existingMeta.content : '';
          const updatedContent = candidate.direction === 'append'
            ? `${existingContent}${insertedText}`
            : `${insertedText}${existingContent}`;

          newTr.insertText(insertedText, safeFrom);
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: candidate.id, kind: 'insert', by: actor })
          );
          writeOffset += insertedText.length;

          metadata = {
            ...metadata,
            [candidate.id]: {
              ...existingMeta,
              kind: 'insert',
              by: actor,
              content: updatedContent,
              status: existingMeta?.status ?? 'pending',
              createdAt: existingMeta?.createdAt ?? new Date().toISOString(),
            },
          };
          metadataChanged = true;

          lastInsertByActor.set(actor, {
            id: candidate.id,
            from: candidate.range.from,
            to: candidate.range.to + insertedText.length,
            by: actor,
            updatedAt: now,
          });
        } else if (whitespaceOnly) {
          // Standalone whitespace, no active candidate: create a tracked suggestion mark.
          const suggestionId = generateMarkId();
          const createdAt = new Date().toISOString();

          newTr.insertText(insertedText, safeFrom);
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: suggestionId, kind: 'insert', by: actor })
          );
          writeOffset += insertedText.length;

          metadata = {
            ...metadata,
            [suggestionId]: buildSuggestionMetadata('insert', actor, insertedText, createdAt),
          };
          metadataChanged = true;

          lastInsertByActor.set(actor, {
            id: suggestionId,
            from: safeFrom,
            to: safeFrom + insertedText.length,
            by: actor,
            updatedAt: now,
          });
        } else {
          // New non-whitespace text, no candidate: create fresh suggestion mark
          const suggestionId = generateMarkId();
          const createdAt = new Date().toISOString();

          newTr.insertText(insertedText, safeFrom);
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: suggestionId, kind: 'insert', by: actor })
          );
          writeOffset += insertedText.length;

          metadata = {
            ...metadata,
            [suggestionId]: buildSuggestionMetadata('insert', actor, insertedText, createdAt),
          };
          metadataChanged = true;

          lastInsertByActor.set(actor, {
            id: suggestionId,
            from: safeFrom,
            to: safeFrom + insertedText.length,
            by: actor,
            updatedAt: now,
          });
        }
      }
      // CASE 3: Replacement (deletion + insertion)
      else if (deletedText && insertedText) {
        lastInsertByActor.delete(actor);
        const existing = detectSuggestionKinds(newTr.doc, safeFrom, safeTo, suggestionType);

        if (existing.hasDelete) {
          // Accept deletion and re-insert as an insertion suggestion.
          newTr.delete(safeFrom, safeTo);
          writeOffset -= deletedText.length;

          const suggestionId = generateMarkId();
          const createdAt = new Date().toISOString();
          newTr.insertText(insertedText, safeFrom);
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: suggestionId, kind: 'insert', by: actor })
          );
          writeOffset += insertedText.length;

          metadata = {
            ...metadata,
            [suggestionId]: buildSuggestionMetadata('insert', actor, insertedText, createdAt),
          };
          metadataChanged = true;
        } else if (existing.hasInsert) {
          // Replace inside a pending insertion - keep it as an insertion suggestion.
          const suggestionId = generateMarkId();
          const createdAt = new Date().toISOString();

          newTr.replaceWith(safeFrom, safeTo, state.schema.text(insertedText));
          newTr.addMark(
            safeFrom,
            safeFrom + insertedText.length,
            suggestionType.create({ id: suggestionId, kind: 'insert', by: actor })
          );
          writeOffset += insertedText.length - deletedText.length;

          metadata = {
            ...metadata,
            [suggestionId]: buildSuggestionMetadata('insert', actor, insertedText, createdAt),
          };
          metadataChanged = true;
        } else {
          // Replace: show the live replacement text and keep the original text in metadata.
          const suggestionId = generateMarkId();
          const createdAt = new Date().toISOString();

          newTr.insertText(insertedText, safeFrom, safeTo);
          newTr.addMark(safeFrom, safeFrom + insertedText.length, suggestionType.create({
            id: suggestionId,
            kind: 'replace',
            by: actor,
          }));
          writeOffset += insertedText.length - deletedText.length;

          metadata = {
            ...metadata,
            [suggestionId]: {
              ...buildSuggestionMetadata('replace', actor, insertedText, createdAt),
              content: insertedText,
              originalQuote: normalizeQuote(deletedText),
              quote: normalizeQuote(insertedText),
              range: { from: safeFrom, to: safeFrom + insertedText.length },
              startRel: undefined,
              endRel: undefined,
            },
          };
          metadataChanged = true;
        }
      }
      // CASE 4: Structural-only change (e.g., paragraph join/split with no text content).
      // Both deletedText and insertedText are empty — this isn't a text edit.
      // Pass through directly and adjust writeOffset for any doc size change.
      else {
        try {
          const sizeBefore = newTr.doc.content.size;
          newTr.step(step);
          writeOffset += newTr.doc.content.size - sizeBefore;
        } catch (e) {
          console.warn('[suggestions] Could not apply structural step:', e);
        }
      }
    } else if (stepJson.stepType === 'replaceAround' || stepJson.stepType === 'addMark' || stepJson.stepType === 'removeMark') {
      // Pass through structural and mark changes directly
      try {
        newTr.step(step);
      } catch (e) {
        console.warn('[suggestions] Could not apply step:', stepJson.stepType, e);
      }
    } else {
      // For other step types, try to apply them directly
      try {
        const result = step.apply(newTr.doc);
        if (result.doc && result.doc !== newTr.doc) {
          const sizeDiff = result.doc.content.size - newTr.doc.content.size;
          newTr.step(step);
          writeOffset += sizeDiff;
        }
      } catch (e) {
        console.warn('[suggestions] Could not apply step:', stepJson.stepType, e);
      }
    }
  }

  if (metadataChanged) {
    newTr.setMeta(marksPluginKey, { type: 'SET_METADATA', metadata });
  }

  // Mark this transaction so authorship tracking skips it
  newTr.setMeta('suggestions-wrapped', true);

  return newTr;
}

/**
 * Check if suggestions are enabled
 */
export function isSuggestionsEnabled(state: EditorState): boolean {
  const pluginState = suggestionsPluginKey.getState(state);
  return pluginState?.enabled ?? false;
}

/**
 * Enable suggestions
 */
export function enableSuggestions(view: { state: EditorState; dispatch: (tr: Transaction) => void }): void {
  const tr = view.state.tr.setMeta(suggestionsPluginKey, { enabled: true });
  view.dispatch(tr);
}

/**
 * Disable suggestions
 */
export function disableSuggestions(view: { state: EditorState; dispatch: (tr: Transaction) => void }): void {
  const tr = view.state.tr.setMeta(suggestionsPluginKey, { enabled: false });
  view.dispatch(tr);
}

/**
 * Toggle suggestions
 */
export function toggleSuggestions(view: { state: EditorState; dispatch: (tr: Transaction) => void }): boolean {
  const enabled = isSuggestionsEnabled(view.state);
  if (enabled) {
    disableSuggestions(view);
  } else {
    enableSuggestions(view);
  }
  return !enabled;
}

/**
 * Create the suggestions plugin
 */
export const suggestionsPlugin = $prose(() => {
  return new Plugin<SuggestionState>({
    key: suggestionsPluginKey,

    state: {
      init(): SuggestionState {
        return { enabled: false };
      },

      apply(tr, value): SuggestionState {
        const meta = tr.getMeta(suggestionsPluginKey);
        if (meta !== undefined) {
          return { ...value, ...meta };
        }
        return value;
      },
    },

    appendTransaction(_trs, oldState, newState) {
      const wasEnabled = suggestionsPluginKey.getState(oldState)?.enabled ?? false;
      const isEnabled = suggestionsPluginKey.getState(newState)?.enabled ?? false;
      if (wasEnabled !== isEnabled) {
        // Emit bridge message on next microtask to avoid dispatch-in-dispatch
        queueMicrotask(() => {
          (window as any).proof?.bridge?.sendMessage('suggestionsChanged', { enabled: isEnabled });
        });
      }
      return null;
    },
  });
});

/**
 * Export all for use in editor
 */
export const suggestionsPlugins = [suggestionsCtx, suggestionsPlugin];
