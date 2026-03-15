/**
 * Keybindings Plugin for Proof
 *
 * Provides keyboard shortcuts for agent invocation:
 * - Cmd+Shift+P: Invoke agent on selection (opens input dialog)
 * - Cmd+Shift+K: Add comment for Proof to review later
 */

import { $prose } from '@milkdown/kit/utils';
import { Plugin, PluginKey, type EditorState } from '@milkdown/kit/prose/state';
import type { EditorView } from '@milkdown/kit/prose/view';
import { keymap } from '@milkdown/kit/prose/keymap';
import {
  comment as addComment,
  accept as acceptSuggestionMark,
  getMarks,
  getActiveMarkId,
  reject as rejectSuggestionMark,
  setActiveMark,
  resolve,
} from './marks';
import {
  getUnresolvedComments,
} from '../../formats/marks';
import { getCurrentActor } from '../actor';
import { proofKeybindingConfig } from '../keybindings-config';
import { getTextForRange } from '../utils/text-range';

// ============================================================================
// Types
// ============================================================================

export interface AgentInputContext {
  selection: string;
  range: { from: number; to: number };
  position: { top: number; left: number };
}

export interface AgentInputCallbacks {
  onSubmit: (prompt: string) => Promise<void>;
  onCancel: () => void;
}

// ============================================================================
// Plugin State
// ============================================================================

const keybindingsKey = new PluginKey('keybindings');

// Callbacks for showing the agent input dialog
let showAgentInputCallback: ((context: AgentInputContext, callbacks: AgentInputCallbacks) => void) | null = null;

function getProofEditorApi(): Window['proof'] | null {
  if (typeof window === 'undefined') return null;
  return window.proof ?? null;
}

function getPendingSuggestionIds(state: EditorState): string[] {
  return getMarks(state)
    .filter((mark) => mark.kind === 'insert' || mark.kind === 'delete' || mark.kind === 'replace')
    .sort((a, b) => (a.range?.from ?? 0) - (b.range?.from ?? 0))
    .map((mark) => mark.id);
}

function getSuggestionReviewFollowupId(state: EditorState, activeId: string): string | null {
  const suggestionIds = getPendingSuggestionIds(state);
  if (suggestionIds.length <= 1) return null;
  const currentIndex = suggestionIds.indexOf(activeId);
  if (currentIndex === -1) return suggestionIds[0] ?? null;
  return suggestionIds[(currentIndex + 1) % suggestionIds.length] ?? null;
}

function persistSuggestionReviewAndAdvance(
  markId: string,
  nextSuggestionId: string | null,
  action: 'accept' | 'reject',
): boolean {
  const proof = getProofEditorApi();
  const persistedAction = action === 'accept'
    ? proof?.markAcceptPersisted
    : proof?.markRejectPersisted;
  if (typeof persistedAction !== 'function') return false;

  void persistedAction.call(proof, markId).then((success) => {
    if (!success || !nextSuggestionId) return;
    if (typeof proof?.navigateToMark === 'function') {
      proof.navigateToMark(nextSuggestionId);
    }
  });
  return true;
}

/**
 * Set the callback for showing the agent input dialog
 */
export function setShowAgentInputCallback(
  callback: (context: AgentInputContext, callbacks: AgentInputCallbacks) => void
): void {
  showAgentInputCallback = callback;
}

// ============================================================================
// Commands
// ============================================================================

/**
 * Invoke agent on selection (Cmd+Shift+P)
 * Opens a floating input dialog for the user to type their prompt
 */
function invokeAgentCommand(
  state: Parameters<typeof keymap>[0] extends Record<string, infer F> ? (F extends (s: infer S, ...args: unknown[]) => boolean ? S : never) : never,
  _dispatch: ((tr: unknown) => void) | undefined,
  view: EditorView | undefined
): boolean {
  if (!view) return false;

  const { from, to } = state.selection;
  const selectedText = getTextForRange(state.doc, { from, to });

  // Get coordinates at selection start for positioning the dialog
  const coords = view.coordsAtPos(from);

  const context: AgentInputContext = {
    selection: selectedText,
    range: { from, to },
    position: { top: coords.top, left: coords.left },
  };

  if (showAgentInputCallback) {
    showAgentInputCallback(context, {
      onSubmit: async (prompt: string) => {
        // This will be wired up by the editor to trigger the agent
        const event = new CustomEvent('proof:invoke-agent', {
          detail: { prompt, context },
        });
        window.dispatchEvent(event);
      },
      onCancel: () => {
        // Dialog cancelled, nothing to do
      },
    });
  } else {
    // Fallback: dispatch event directly if no UI callback set
    const event = new CustomEvent('proof:invoke-agent', {
      detail: { prompt: '', context, showDialog: true },
    });
    window.dispatchEvent(event);
  }

  return true;
}

/**
 * Add comment for Proof to review (Cmd+Shift+K)
 * Tags the selection with a comment for the agent to review later
 */
function addProofCommentCommand(
  state: Parameters<typeof keymap>[0] extends Record<string, infer F> ? (F extends (s: infer S, ...args: unknown[]) => boolean ? S : never) : never,
  _dispatch: ((tr: unknown) => void) | undefined,
  view: EditorView | undefined
): boolean {
  if (!view) return false;

  const { from, to } = state.selection;
  const selectedText = getTextForRange(state.doc, { from, to });

  if (!selectedText.trim()) {
    // No selection, don't create empty comment
    return false;
  }

  // Create comment mark tagged for Proof review
  const actor = getCurrentActor();
  addComment(view, selectedText, actor, '[For @proof to review]', { from, to });

  return true;
}

/**
 * Navigate to the next unresolved comment (Mod-])
 * Cycles through comments sorted by document position, wrapping around.
 */
function navigateNextComment(
  state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  view: EditorView | undefined
): boolean {
  if (!view) return false;

  const allMarks = getMarks(state);
  const comments = getUnresolvedComments(allMarks);
  if (comments.length === 0) return false;

  const sorted = [...comments].sort((a, b) => (a.range?.from ?? 0) - (b.range?.from ?? 0));
  const activeId = getActiveMarkId(state);
  const currentIndex = sorted.findIndex((comment) => comment.id === activeId);
  const nextIndex = (currentIndex + 1) % sorted.length;
  const mark = sorted[nextIndex];

  setActiveMark(view, mark.id);

  // Scroll to the mark
  if (mark.range) {
    const coords = view.coordsAtPos(mark.range.from);
    if (coords) {
      const editorRect = view.dom.getBoundingClientRect();
      const scrollTop = view.dom.scrollTop;
      const targetY = coords.top - editorRect.top + scrollTop - (editorRect.height / 3);
      view.dom.scrollTo({ top: Math.max(0, targetY), behavior: 'smooth' });
    }
  }

  return true;
}

/**
 * Navigate to the previous unresolved comment (Mod-[)
 * Cycles backwards through comments sorted by document position, wrapping around.
 */
function navigatePrevComment(
  state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  view: EditorView | undefined
): boolean {
  if (!view) return false;

  const allMarks = getMarks(state);
  const comments = getUnresolvedComments(allMarks);
  if (comments.length === 0) return false;

  const sorted = [...comments].sort((a, b) => (a.range?.from ?? 0) - (b.range?.from ?? 0));
  const activeId = getActiveMarkId(state);
  const currentIndex = sorted.findIndex((comment) => comment.id === activeId);
  const prevIndex = currentIndex <= 0
    ? sorted.length - 1
    : currentIndex - 1;
  const mark = sorted[prevIndex];

  setActiveMark(view, mark.id);

  // Scroll to the mark
  if (mark.range) {
    const coords = view.coordsAtPos(mark.range.from);
    if (coords) {
      const editorRect = view.dom.getBoundingClientRect();
      const scrollTop = view.dom.scrollTop;
      const targetY = coords.top - editorRect.top + scrollTop - (editorRect.height / 3);
      view.dom.scrollTo({ top: Math.max(0, targetY), behavior: 'smooth' });
    }
  }

  return true;
}

/**
 * Resolve the active comment (Mod-Shift-r)
 * If there's an active comment popover, resolves the entire thread.
 * Silent no-op if no active comment.
 */
function resolveActiveComment(
  state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  view: EditorView | undefined
): boolean {
  if (!view) return false;

  const activeId = getActiveMarkId(state);
  if (!activeId) return false;

  // Verify the active mark is a comment
  const allMarks = getMarks(state);
  const mark = allMarks.find(m => m.id === activeId);
  if (!mark || mark.kind !== 'comment') return false;

  resolve(view, activeId);
  setActiveMark(view, null);
  return true;
}

function undoCommand(
  _state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  _view: EditorView | undefined
): boolean {
  const proof = getProofEditorApi();
  if (!proof?.undo) return false;
  return proof.undo();
}

function redoCommand(
  _state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  _view: EditorView | undefined
): boolean {
  const proof = getProofEditorApi();
  if (!proof?.redo) return false;
  return proof.redo();
}

function navigateNextSuggestionCommand(
  _state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  _view: EditorView | undefined
): boolean {
  const proof = getProofEditorApi();
  if (!proof?.navigateToNextSuggestion) return false;
  proof.navigateToNextSuggestion();
  return true;
}

function navigatePrevSuggestionCommand(
  _state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  _view: EditorView | undefined
): boolean {
  const proof = getProofEditorApi();
  if (!proof?.navigateToPrevSuggestion) return false;
  proof.navigateToPrevSuggestion();
  return true;
}

function acceptActiveSuggestionCommand(
  state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  view: EditorView | undefined
): boolean {
  if (!view) return false;
  const activeId = getActiveMarkId(state);
  if (!activeId) return false;
  const activeMark = getMarks(state).find((mark) => mark.id === activeId);
  if (!activeMark || (activeMark.kind !== 'insert' && activeMark.kind !== 'delete' && activeMark.kind !== 'replace')) {
    return false;
  }
  const nextSuggestionId = getSuggestionReviewFollowupId(state, activeId);

  const proof = getProofEditorApi();
  if (proof?.markAcceptPersisted) {
    const handled = acceptSuggestionMark(view, activeId);
    if (!handled) return false;
    persistSuggestionReviewAndAdvance(activeId, nextSuggestionId, 'accept');
    return true;
  }
  if (proof?.markAccept) {
    const handled = proof.markAccept(activeId);
    if (handled && nextSuggestionId && typeof proof.navigateToMark === 'function') {
      proof.navigateToMark(nextSuggestionId);
    }
    return handled;
  }
  const handled = acceptSuggestionMark(view, activeId);
  if (handled && nextSuggestionId && typeof proof?.navigateToMark === 'function') {
    proof.navigateToMark(nextSuggestionId);
  }
  return handled;
}

function rejectActiveSuggestionCommand(
  state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  view: EditorView | undefined
): boolean {
  if (!view) return false;
  const activeId = getActiveMarkId(state);
  if (!activeId) return false;
  const activeMark = getMarks(state).find((mark) => mark.id === activeId);
  if (!activeMark || (activeMark.kind !== 'insert' && activeMark.kind !== 'delete' && activeMark.kind !== 'replace')) {
    return false;
  }
  const nextSuggestionId = getSuggestionReviewFollowupId(state, activeId);

  const proof = getProofEditorApi();
  if (proof?.markRejectPersisted) {
    const handled = rejectSuggestionMark(view, activeId);
    if (!handled) return false;
    persistSuggestionReviewAndAdvance(activeId, nextSuggestionId, 'reject');
    return true;
  }
  if (proof?.markReject) {
    const handled = proof.markReject(activeId);
    if (handled && nextSuggestionId && typeof proof.navigateToMark === 'function') {
      proof.navigateToMark(nextSuggestionId);
    }
    return handled;
  }
  const handled = rejectSuggestionMark(view, activeId);
  if (handled && nextSuggestionId && typeof proof?.navigateToMark === 'function') {
    proof.navigateToMark(nextSuggestionId);
  }
  return handled;
}

function toggleSuggestionsCommand(
  _state: EditorState,
  _dispatch: ((tr: unknown) => void) | undefined,
  _view: EditorView | undefined
): boolean {
  const proof = getProofEditorApi();
  if (!proof?.toggleSuggestions) return false;
  proof.toggleSuggestions();
  return true;
}

// ============================================================================
// Quick Actions
// ============================================================================

export type QuickAction = 'fix-grammar' | 'improve-clarity' | 'make-shorter';

const quickActionPrompts: Record<QuickAction, string> = {
  'fix-grammar': 'Fix any grammar issues in this text',
  'improve-clarity': 'Improve the clarity of this text while keeping the meaning',
  'make-shorter': 'Make this text more concise without losing important information',
};

/**
 * Execute a quick action on the selection
 */
export function executeQuickAction(view: EditorView, action: QuickAction): void {
  const { from, to } = view.state.selection;
  const selectedText = getTextForRange(view.state.doc, { from, to });

  if (!selectedText.trim()) {
    return;
  }

  const prompt = quickActionPrompts[action];
  const coords = view.coordsAtPos(from);

  const context: AgentInputContext = {
    selection: selectedText,
    range: { from, to },
    position: { top: coords.top, left: coords.left },
  };

  // Dispatch event to trigger agent with the quick action prompt
  const event = new CustomEvent('proof:invoke-agent', {
    detail: { prompt, context },
  });
  window.dispatchEvent(event);
}

// ============================================================================
// Keymap
// ============================================================================

const agentKeymap = keymap({
  [proofKeybindingConfig.invokeAgent]: invokeAgentCommand,
  [proofKeybindingConfig.addProofComment]: addProofCommentCommand,
  [proofKeybindingConfig.undo]: undoCommand,
  [proofKeybindingConfig.redo]: redoCommand,
  [proofKeybindingConfig.nextComment]: navigateNextComment,
  [proofKeybindingConfig.prevComment]: navigatePrevComment,
  [proofKeybindingConfig.nextSuggestion]: navigateNextSuggestionCommand,
  [proofKeybindingConfig.prevSuggestion]: navigatePrevSuggestionCommand,
  [proofKeybindingConfig.acceptSuggestionAndNext]: acceptActiveSuggestionCommand,
  [proofKeybindingConfig.rejectSuggestionAndNext]: rejectActiveSuggestionCommand,
  [proofKeybindingConfig.toggleTrackChanges]: toggleSuggestionsCommand,
  [proofKeybindingConfig.resolveComment]: resolveActiveComment,
});

// ============================================================================
// Plugin
// ============================================================================

export const keybindingsPlugin = $prose(() => {
  return new Plugin({
    key: keybindingsKey,
    props: {
      handleKeyDown: agentKeymap.props.handleKeyDown,
    },
  });
});

export default keybindingsPlugin;
