/**
 * Context Menu for Proof
 *
 * Provides right-click menu with agent options and suggestion review actions:
 * - Accept / Reject / Review change when right-clicking a tracked change
 * - Ask Proof... (opens input dialog)
 * - Quick Actions submenu
 * - Add Comment for Proof
 */

import type { EditorView } from '@milkdown/kit/prose/view';
import { showAgentInputDialog } from './agent-input-dialog';
import {
  accept as acceptSuggestionMark,
  comment as addComment,
  getMarks,
  reject as rejectSuggestionMark,
} from '../editor/plugins/marks';
import { getCurrentActor } from '../editor/actor';
import { proofKeybindingConfig } from '../editor/keybindings-config';
import type { AgentInputContext } from '../editor/plugins/keybindings';
import { getTextForRange } from '../editor/utils/text-range';

// ============================================================================
// Types
// ============================================================================

interface ContextMenuState {
  isOpen: boolean;
  element: HTMLElement | null;
  shortcutsElement: HTMLElement | null;
  editorView: EditorView | null;
  selectionContext: {
    text: string;
    from: number;
    to: number;
  } | null;
  suggestionContext: {
    markId: string;
    kind: 'insert' | 'delete' | 'replace';
  } | null;
}

type QuickAction = 'fix-grammar' | 'improve-clarity' | 'make-shorter';
type ShortcutHelpEntry = {
  label: string;
  binding: string;
};

// ============================================================================
// State
// ============================================================================

const state: ContextMenuState = {
  isOpen: false,
  element: null,
  shortcutsElement: null,
  editorView: null,
  selectionContext: null,
  suggestionContext: null,
};

function getProofEditorApi(): Window['proof'] | null {
  if (typeof window === 'undefined') return null;
  return window.proof ?? null;
}

function resolveSuggestionContext(
  view: EditorView,
  target: EventTarget | null,
): ContextMenuState['suggestionContext'] {
  const targetElement = target instanceof Element
    ? target
    : target instanceof Node
      ? target.parentElement
      : null;
  if (!(targetElement instanceof HTMLElement)) return null;
  const markEl = targetElement.closest('[data-mark-id]') as HTMLElement | null;
  const markId = markEl?.dataset.markId ?? '';
  if (!markId) return null;
  const mark = getMarks(view.state).find((item) => item.id === markId);
  if (!mark || (mark.kind !== 'insert' && mark.kind !== 'delete' && mark.kind !== 'replace')) {
    return null;
  }
  return {
    markId,
    kind: mark.kind,
  };
}

// ============================================================================
// Menu Element
// ============================================================================

function createSuggestionActionsMarkup(
  suggestionContext: ContextMenuState['suggestionContext'],
): string {
  if (!suggestionContext) return '';

  const label = suggestionContext.kind === 'replace'
    ? 'replacement'
    : suggestionContext.kind === 'insert'
      ? 'insertion'
      : 'deletion';

  return `
      <button class="proof-context-menu-item" data-action="review-suggestion">
        <span class="proof-context-menu-icon">↗</span>
        <span>Review ${label}</span>
      </button>
      <button class="proof-context-menu-item" data-action="accept-suggestion">
        <span class="proof-context-menu-icon">✓</span>
        <span>Accept change</span>
      </button>
      <button class="proof-context-menu-item" data-action="reject-suggestion">
        <span class="proof-context-menu-icon">✕</span>
        <span>Reject change</span>
      </button>
      <div class="proof-context-menu-separator"></div>
  `;
}

function createMenuElement(
  suggestionContext: ContextMenuState['suggestionContext'],
): HTMLElement {
  const menu = document.createElement('div');
  menu.className = 'proof-context-menu';
  menu.innerHTML = `
    <div class="proof-context-menu-items">
      ${createSuggestionActionsMarkup(suggestionContext)}
      <button class="proof-context-menu-item" data-action="ask-proof">
        <span class="proof-context-menu-icon">💬</span>
        <span>Ask Proof...</span>
        <span class="proof-context-menu-shortcut">⇧⌘P</span>
      </button>
      <div class="proof-context-menu-item has-submenu" data-action="quick-actions">
        <span class="proof-context-menu-icon">⚡</span>
        <span>Quick Actions</span>
        <span class="proof-context-menu-arrow">▶</span>
        <div class="proof-context-submenu">
          <button class="proof-context-menu-item" data-quick-action="fix-grammar">
            Fix grammar
          </button>
          <button class="proof-context-menu-item" data-quick-action="improve-clarity">
            Improve clarity
          </button>
          <button class="proof-context-menu-item" data-quick-action="make-shorter">
            Make it shorter
          </button>
        </div>
      </div>
      <button class="proof-context-menu-item" data-action="show-shortcuts">
        <span class="proof-context-menu-icon">⌘</span>
        <span>Keyboard shortcuts</span>
      </button>
      <div class="proof-context-menu-separator"></div>
      <button class="proof-context-menu-item" data-action="add-comment">
        <span class="proof-context-menu-icon">📝</span>
        <span>Add Comment for Proof</span>
        <span class="proof-context-menu-shortcut">⇧⌘K</span>
      </button>
    </div>
  `;

  // Add styles if not already added
  if (!document.getElementById('proof-context-menu-styles')) {
    const style = document.createElement('style');
    style.id = 'proof-context-menu-styles';
    style.textContent = `
      .proof-context-menu {
        position: fixed;
        z-index: 10001;
        background: var(--proof-bg, #ffffff);
        border: 1px solid var(--proof-border, #e5e7eb);
        border-radius: 8px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
        min-width: 220px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        font-size: 13px;
        padding: 4px 0;
        opacity: 0;
        transform: scale(0.95);
        transform-origin: top left;
        transition: opacity 0.1s ease, transform 0.1s ease;
      }

      .proof-context-menu.visible {
        opacity: 1;
        transform: scale(1);
      }

      .proof-context-menu-items {
        display: flex;
        flex-direction: column;
      }

      .proof-context-menu-item {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        background: none;
        border: none;
        text-align: left;
        cursor: pointer;
        color: var(--proof-text, #1f2937);
        width: 100%;
        position: relative;
      }

      .proof-context-menu-item:hover {
        background: var(--proof-bg-hover, #f3f4f6);
      }

      .proof-context-menu-item:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }

      .proof-context-menu-icon {
        width: 20px;
        text-align: center;
        font-size: 14px;
      }

      .proof-context-menu-shortcut {
        margin-left: auto;
        color: var(--proof-text-muted, #9ca3af);
        font-size: 11px;
      }

      .proof-context-menu-arrow {
        margin-left: auto;
        color: var(--proof-text-muted, #9ca3af);
        font-size: 10px;
      }

      .proof-context-menu-separator {
        height: 1px;
        background: var(--proof-border, #e5e7eb);
        margin: 4px 0;
      }

      .proof-context-menu-item.has-submenu {
        position: relative;
      }

      .proof-context-submenu {
        position: absolute;
        left: 100%;
        top: -4px;
        background: var(--proof-bg, #ffffff);
        border: 1px solid var(--proof-border, #e5e7eb);
        border-radius: 8px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.15);
        min-width: 160px;
        padding: 4px 0;
        opacity: 0;
        visibility: hidden;
        transform: translateX(-8px);
        transition: opacity 0.1s ease, transform 0.1s ease, visibility 0.1s;
      }

      .proof-context-menu-item.has-submenu:hover .proof-context-submenu {
        opacity: 1;
        visibility: visible;
        transform: translateX(0);
      }

      .proof-context-shortcuts-popover {
        position: fixed;
        z-index: 10002;
        background: var(--proof-bg, #ffffff);
        border: 1px solid var(--proof-border, #e5e7eb);
        border-radius: 10px;
        box-shadow: 0 10px 28px rgba(0, 0, 0, 0.16);
        min-width: 280px;
        max-width: min(360px, calc(100vw - 24px));
        padding: 10px 0;
        opacity: 0;
        transform: scale(0.97);
        transform-origin: top left;
        transition: opacity 0.12s ease, transform 0.12s ease;
      }

      .proof-context-shortcuts-popover.visible {
        opacity: 1;
        transform: scale(1);
      }

      .proof-context-shortcuts-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 0 14px 8px;
        font-weight: 600;
        color: var(--proof-text, #1f2937);
      }

      .proof-context-shortcuts-title {
        font-size: 13px;
      }

      .proof-context-shortcuts-close {
        border: none;
        background: none;
        color: var(--proof-text-muted, #9ca3af);
        cursor: pointer;
        font-size: 16px;
        line-height: 1;
        padding: 0;
      }

      .proof-context-shortcuts-close:hover {
        color: var(--proof-text, #1f2937);
      }

      .proof-context-shortcuts-list {
        display: flex;
        flex-direction: column;
        gap: 2px;
      }

      .proof-context-shortcuts-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding: 6px 14px;
        color: var(--proof-text, #1f2937);
      }

      .proof-context-shortcuts-label {
        font-size: 12px;
      }

      .proof-context-shortcuts-binding {
        color: var(--proof-text-muted, #6b7280);
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 11px;
        white-space: nowrap;
      }

      /* Dark mode support */
      @media (prefers-color-scheme: dark) {
        .proof-context-menu {
          --proof-bg: #1f2937;
          --proof-bg-hover: #374151;
          --proof-border: #4b5563;
          --proof-text: #f9fafb;
          --proof-text-muted: #9ca3af;
        }
      }
    `;
    document.head.appendChild(style);
  }

  return menu;
}

// ============================================================================
// Menu Positioning
// ============================================================================

function positionMenu(menu: HTMLElement, x: number, y: number): void {
  const margin = 8;
  const viewportW = window.innerWidth;
  const viewportH = window.innerHeight;

  // Temporarily show to get dimensions
  menu.style.visibility = 'hidden';
  menu.style.left = '0px';
  menu.style.top = '0px';
  document.body.appendChild(menu);

  const rect = menu.getBoundingClientRect();

  // Adjust position to stay within viewport
  let left = x;
  let top = y;

  if (left + rect.width > viewportW - margin) {
    left = viewportW - rect.width - margin;
  }
  if (left < margin) {
    left = margin;
  }

  if (top + rect.height > viewportH - margin) {
    top = viewportH - rect.height - margin;
  }
  if (top < margin) {
    top = margin;
  }

  menu.style.visibility = '';
  menu.style.left = `${left}px`;
  menu.style.top = `${top}px`;
}

function positionAdjacentPopover(popover: HTMLElement, anchor: DOMRect): void {
  const margin = 8;
  const viewportW = window.innerWidth;
  const viewportH = window.innerHeight;

  popover.style.visibility = 'hidden';
  popover.style.left = '0px';
  popover.style.top = '0px';
  document.body.appendChild(popover);

  const rect = popover.getBoundingClientRect();
  let left = anchor.right + 8;
  if (left + rect.width > viewportW - margin) {
    left = anchor.left - rect.width - 8;
  }
  if (left < margin) {
    left = Math.max(margin, Math.min(viewportW - rect.width - margin, anchor.left));
  }

  let top = anchor.top;
  if (top + rect.height > viewportH - margin) {
    top = viewportH - rect.height - margin;
  }
  if (top < margin) {
    top = margin;
  }

  popover.style.visibility = '';
  popover.style.left = `${left}px`;
  popover.style.top = `${top}px`;
}

function isMacPlatform(): boolean {
  if (typeof navigator === 'undefined') return true;
  return /Mac|iPhone|iPad|iPod/.test(navigator.platform);
}

function formatShortcutBinding(binding: string): string {
  const isMac = isMacPlatform();
  const formattedTokens = binding.split('-').map((token) => {
    if (token === 'Mod') return isMac ? 'Cmd' : 'Ctrl';
    if (token === 'Alt') return isMac ? 'Option' : 'Alt';
    if (token === 'Shift') return 'Shift';
    if (token.length === 1) return token.toUpperCase();
    return token;
  });
  return formattedTokens.join('+');
}

function getShortcutHelpEntries(): ShortcutHelpEntry[] {
  return [
    { label: 'Ask Proof', binding: proofKeybindingConfig.invokeAgent },
    { label: 'Add comment for Proof', binding: proofKeybindingConfig.addProofComment },
    { label: 'Undo', binding: proofKeybindingConfig.undo },
    { label: 'Redo', binding: proofKeybindingConfig.redo },
    { label: 'Next comment', binding: proofKeybindingConfig.nextComment },
    { label: 'Previous comment', binding: proofKeybindingConfig.prevComment },
    { label: 'Next change', binding: proofKeybindingConfig.nextSuggestion },
    { label: 'Previous change', binding: proofKeybindingConfig.prevSuggestion },
    { label: 'Accept change', binding: proofKeybindingConfig.acceptSuggestionAndNext },
    { label: 'Reject change', binding: proofKeybindingConfig.rejectSuggestionAndNext },
    { label: 'Toggle Track Changes', binding: proofKeybindingConfig.toggleTrackChanges },
    { label: 'Resolve comment', binding: proofKeybindingConfig.resolveComment },
  ];
}

function closeShortcutsPopover(): void {
  if (!state.shortcutsElement) return;
  const popover = state.shortcutsElement;
  popover.classList.remove('visible');
  window.setTimeout(() => {
    if (state.shortcutsElement === popover) {
      state.shortcutsElement = null;
    }
    if (popover.parentNode) {
      popover.parentNode.removeChild(popover);
    }
  }, 100);
}

function showShortcutsPopover(): void {
  if (!state.element) return;
  closeShortcutsPopover();

  const popover = document.createElement('div');
  popover.className = 'proof-context-shortcuts-popover';
  const entries = getShortcutHelpEntries();
  popover.innerHTML = `
    <div class="proof-context-shortcuts-header">
      <span class="proof-context-shortcuts-title">Keyboard shortcuts</span>
      <button class="proof-context-shortcuts-close" type="button" aria-label="Close keyboard shortcuts">×</button>
    </div>
    <div class="proof-context-shortcuts-list">
      ${entries.map((entry) => `
        <div class="proof-context-shortcuts-row">
          <span class="proof-context-shortcuts-label">${entry.label}</span>
          <span class="proof-context-shortcuts-binding">${formatShortcutBinding(entry.binding)}</span>
        </div>
      `).join('')}
    </div>
  `;

  state.shortcutsElement = popover;
  positionAdjacentPopover(popover, state.element.getBoundingClientRect());
  const closeButton = popover.querySelector('.proof-context-shortcuts-close') as HTMLButtonElement | null;
  closeButton?.addEventListener('click', (event) => {
    event.preventDefault();
    event.stopPropagation();
    closeShortcutsPopover();
  });
  requestAnimationFrame(() => {
    popover.classList.add('visible');
  });
}

// ============================================================================
// Event Handlers
// ============================================================================

function handleKeyDown(e: KeyboardEvent): void {
  if (!state.isOpen && !state.shortcutsElement) return;

  if (e.key === 'Escape') {
    closeShortcutsPopover();
    closeMenu();
    e.preventDefault();
    e.stopPropagation();
  }
}

function handleClickOutside(e: MouseEvent): void {
  if (!state.isOpen || !state.element) return;

  if (state.shortcutsElement?.contains(e.target as Node)) {
    return;
  }
  if (!state.element.contains(e.target as Node)) {
    closeShortcutsPopover();
    closeMenu();
  }
}

async function runSuggestionAction(action: 'accept' | 'reject', markId: string, view: EditorView): Promise<void> {
  const proof = getProofEditorApi();
  if (action === 'accept') {
    if (typeof proof?.markAcceptPersisted === 'function') {
      await proof.markAcceptPersisted(markId);
      return;
    }
    if (typeof proof?.markAccept === 'function') {
      proof.markAccept(markId);
      return;
    }
    acceptSuggestionMark(view, markId);
    return;
  }

  if (typeof proof?.markRejectPersisted === 'function') {
    await proof.markRejectPersisted(markId);
    return;
  }
  if (typeof proof?.markReject === 'function') {
    proof.markReject(markId);
    return;
  }
  rejectSuggestionMark(view, markId);
}

function reviewSuggestion(markId: string): void {
  const proof = getProofEditorApi();
  if (typeof proof?.navigateToMark === 'function') {
    proof.navigateToMark(markId);
  }
}

async function handleAction(action: string): Promise<void> {
  if (!state.editorView) return;
  const view = state.editorView;
  const selectionContext = state.selectionContext;
  const suggestionContext = state.suggestionContext;

  if (action === 'accept-suggestion' || action === 'reject-suggestion' || action === 'review-suggestion') {
    if (!suggestionContext) {
      closeMenu();
      return;
    }
    const { markId } = suggestionContext;
    closeMenu();
    if (action === 'review-suggestion') {
      reviewSuggestion(markId);
      return;
    }
    await runSuggestionAction(action === 'accept-suggestion' ? 'accept' : 'reject', markId, view);
    return;
  }

  if (!selectionContext) {
    closeMenu();
    return;
  }

  const { text, from, to } = selectionContext;
  const coords = view.coordsAtPos(from);

  switch (action) {
    case 'show-shortcuts': {
      showShortcutsPopover();
      return;
    }

    case 'ask-proof': {
      const context: AgentInputContext = {
        selection: text,
        range: { from, to },
        position: { top: coords.top, left: coords.left },
      };
      showAgentInputDialog(context, {
        onSubmit: async (prompt: string) => {
          const event = new CustomEvent('proof:invoke-agent', {
            detail: { prompt, context },
          });
          window.dispatchEvent(event);
        },
        onCancel: () => {},
      });
      break;
    }

    case 'add-comment': {
      if (text.trim()) {
        const actor = getCurrentActor();
        addComment(view, text, actor, '[For @proof to review]', { from, to });
      }
      break;
    }
  }

  closeMenu();
}

function handleQuickAction(action: QuickAction): void {
  if (!state.editorView || !state.selectionContext) return;

  const { text, from, to } = state.selectionContext;
  const coords = state.editorView.coordsAtPos(from);

  const prompts: Record<QuickAction, string> = {
    'fix-grammar': 'Fix any grammar issues in this text',
    'improve-clarity': 'Improve the clarity of this text while keeping the meaning',
    'make-shorter': 'Make this text more concise without losing important information',
  };

  const context: AgentInputContext = {
    selection: text,
    range: { from, to },
    position: { top: coords.top, left: coords.left },
  };

  const event = new CustomEvent('proof:invoke-agent', {
    detail: { prompt: prompts[action], context },
  });
  window.dispatchEvent(event);

  closeMenu();
}

// ============================================================================
// Public API
// ============================================================================

/**
 * Show the context menu at the given position
 */
export function showContextMenu(
  view: EditorView,
  x: number,
  y: number,
  target?: EventTarget | null,
): void {
  // Close any existing menu
  if (state.isOpen) {
    closeMenu();
  }

  // Get selection context
  const { from, to } = view.state.selection;
  const selectedText = getTextForRange(view.state.doc, { from, to });

  state.editorView = view;
  state.selectionContext = {
    text: selectedText,
    from,
    to,
  };
  state.suggestionContext = resolveSuggestionContext(view, target ?? null);

  // Create and position menu
  const menu = createMenuElement(state.suggestionContext);
  state.element = menu;
  state.isOpen = true;

  positionMenu(menu, x, y);

  // Animate in
  requestAnimationFrame(() => {
    menu.classList.add('visible');
  });

  // Disable items if no selection
  if (!selectedText.trim()) {
    const items = menu.querySelectorAll('[data-action="ask-proof"], [data-action="quick-actions"], [data-action="add-comment"]');
    items.forEach((item) => {
      (item as HTMLButtonElement).disabled = true;
    });
  }

  // Wire up event handlers
  const askProofBtn = menu.querySelector('[data-action="ask-proof"]') as HTMLButtonElement;
  const addCommentBtn = menu.querySelector('[data-action="add-comment"]') as HTMLButtonElement;
  const showShortcutsBtn = menu.querySelector('[data-action="show-shortcuts"]') as HTMLButtonElement;
  const acceptSuggestionBtn = menu.querySelector('[data-action="accept-suggestion"]') as HTMLButtonElement | null;
  const rejectSuggestionBtn = menu.querySelector('[data-action="reject-suggestion"]') as HTMLButtonElement | null;
  const reviewSuggestionBtn = menu.querySelector('[data-action="review-suggestion"]') as HTMLButtonElement | null;
  const quickActionBtns = menu.querySelectorAll('[data-quick-action]');

  askProofBtn?.addEventListener('click', () => {
    void handleAction('ask-proof');
  });
  showShortcutsBtn?.addEventListener('click', () => {
    void handleAction('show-shortcuts');
  });
  addCommentBtn?.addEventListener('click', () => {
    void handleAction('add-comment');
  });
  acceptSuggestionBtn?.addEventListener('click', () => {
    void handleAction('accept-suggestion');
  });
  rejectSuggestionBtn?.addEventListener('click', () => {
    void handleAction('reject-suggestion');
  });
  reviewSuggestionBtn?.addEventListener('click', () => {
    void handleAction('review-suggestion');
  });

  quickActionBtns.forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const action = (btn as HTMLElement).dataset.quickAction as QuickAction;
      handleQuickAction(action);
    });
  });

  // Global event listeners
  document.addEventListener('keydown', handleKeyDown, true);
  document.addEventListener('mousedown', handleClickOutside, true);
}

/**
 * Close the context menu
 */
export function closeMenu(): void {
  closeShortcutsPopover();
  if (!state.element) return;

  state.element.classList.remove('visible');

  setTimeout(() => {
    if (state.element && state.element.parentNode) {
      state.element.parentNode.removeChild(state.element);
    }
    state.element = null;
    state.shortcutsElement = null;
    state.isOpen = false;
    state.editorView = null;
    state.selectionContext = null;
    state.suggestionContext = null;
  }, 100);

  document.removeEventListener('keydown', handleKeyDown, true);
  document.removeEventListener('mousedown', handleClickOutside, true);
}

/**
 * Check if context menu is currently open
 */
export function isContextMenuOpen(): boolean {
  return state.isOpen;
}

/**
 * Initialize context menu for the editor
 * Sets up right-click handler
 */
export function initContextMenu(view: EditorView): () => void {
  const handleContextMenu = (e: MouseEvent) => {
    // Only show our menu if clicking in the editor
    if (view.dom.contains(e.target as Node)) {
      e.preventDefault();
      showContextMenu(view, e.clientX, e.clientY, e.target);
    }
  };

  view.dom.addEventListener('contextmenu', handleContextMenu);

  // Return cleanup function
  return () => {
    view.dom.removeEventListener('contextmenu', handleContextMenu);
    closeMenu();
  };
}
