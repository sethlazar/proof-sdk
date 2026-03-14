import { $prose } from '@milkdown/kit/utils';
import { Plugin, PluginKey } from '@milkdown/kit/prose/state';
import type { EditorView } from '@milkdown/kit/prose/view';

import {
  comment as addComment,
  reply as replyToComment,
  resolve as resolveComment,
  unresolve as unresolveComment,
  accept as acceptSuggestion,
  reject as rejectSuggestion,
  flag,
  getMarks,
  resolveMarks,
  deleteMark,
  setActiveMark,
  getActiveMarkId,
  setComposeAnchorRange,
  suggestReplace,
  getSuggestionDisplayMode,
  type MarkRange,
} from './marks';
import {
  getThread,
  getActorName,
  getPendingSuggestions,
  type Mark,
  type CommentData,
  type InsertData,
  type ReplaceData,
} from '../../formats/marks';
import { getCurrentActor } from '../actor';
import { shouldUseCommentUiV2 } from './comment-ui-mode';
import {
  getVisualViewportHeight,
  getViewportOffset,
} from './mark-popover-viewport';
import { isMobileTouch } from './mobile-detect';
import { canCommentInRuntime, canEditInRuntime } from './share-permissions';
import { resolveQuoteRange } from '../utils/text-range';

const markPopoverKey = new PluginKey('mark-popover');
const controllers = new WeakMap<EditorView, MarkPopoverController>();
type PopoverMode = 'thread' | 'suggestion' | 'composer' | null;
type RenderMode = 'legacy-popover' | 'mobile-sheet' | 'desktop-side-panel';
type ThreadFocusMode = 'reply-box' | 'sheet' | 'none';

export type CommentPopoverDraftSnapshot =
  | {
    mode: 'composer';
    range: MarkRange;
    by: string;
    text: string;
  }
  | {
    mode: 'thread';
    markId: string;
    by: string;
    text: string;
  };

/** Number of rAF frames to poll after iOS keyboard dismiss before stopping viewport sync. */
const VIEWPORT_SYNC_FRAMES = 6;
/** Extra bottom padding (px) below the mobile strip to keep content visible. */
const MOBILE_STRIP_PADDING_EXTRA = 20;
const MOBILE_SELECTION_POLL_MS = 120;
const MOBILE_SELECTION_POLL_WINDOW_MS = 1800;
const SUGGESTION_HOVER_OPEN_DELAY_MS = 120;
const SUGGESTION_HOVER_CLOSE_DELAY_MS = 180;
const REVIEW_ACTION_RETRY_DELAY_MS = 150;
const REVIEW_ACTION_MAX_RETRIES = 12;

type VisibleComment = {
  mark: Mark;
  range: MarkRange;
};

type MobileCommentData = {
  nearby: VisibleComment[];
  all: VisibleComment[];
  nearbyCount: number;
  totalCount: number;
};

type TouchSafeButtonOptions = {
  preventTouchPointerDown?: boolean;
  stopPointerDownPropagation?: boolean;
  stopClickPropagation?: boolean;
  onPointerDown?: (event: PointerEvent) => void;
};

function formatTimestamp(iso: string | undefined): string {
  if (!iso) return '';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  return date.toLocaleString();
}

function isMacPlatform(): boolean {
  if (typeof navigator === 'undefined') return false;
  const platform = navigator.platform || '';
  const userAgent = navigator.userAgent || '';
  return /Mac|iPhone|iPad|iPod/i.test(platform) || /Mac/i.test(userAgent);
}

function hasReviewShortcutModifiers(event: KeyboardEvent): boolean {
  const primaryModifierPressed = isMacPlatform()
    ? event.metaKey && !event.ctrlKey
    : event.ctrlKey && !event.metaKey;
  return primaryModifierPressed && event.altKey && !event.shiftKey;
}

function matchesReviewShortcut(
  event: KeyboardEvent,
  options: { key?: string; code?: string },
): boolean {
  if (!hasReviewShortcutModifiers(event)) return false;
  const normalizedKey = event.key.length === 1 ? event.key.toLowerCase() : event.key;
  if (options.key && normalizedKey === options.key.toLowerCase()) return true;
  if (options.code && event.code === options.code) return true;
  return false;
}

function resolveAnchorRange(view: EditorView, mark: Mark, pos?: number | null): MarkRange | null {
  const [resolved] = resolveMarks(view.state.doc, [mark]);
  const ranges = resolved?.resolvedRanges ?? (resolved?.resolvedRange ? [resolved.resolvedRange] : []);
  if (!ranges.length) return null;
  if (typeof pos === 'number') {
    const containing = ranges.find(range => pos >= range.from && pos <= range.to);
    if (containing) return containing;
  }
  return ranges[0] ?? null;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(value, max));
}

function installTouchSafeButton(
  button: HTMLButtonElement,
  onClick: (event: MouseEvent) => void,
  options: TouchSafeButtonOptions = {}
): void {
  const {
    preventTouchPointerDown = true,
    stopPointerDownPropagation = true,
    stopClickPropagation = true,
    onPointerDown,
  } = options;

  button.addEventListener('pointerdown', event => {
    if (preventTouchPointerDown && event.pointerType === 'touch') {
      event.preventDefault();
    }
    if (stopPointerDownPropagation) {
      event.stopPropagation();
    }
    onPointerDown?.(event);
  });

  button.addEventListener('click', event => {
    event.preventDefault();
    if (stopClickPropagation) {
      event.stopPropagation();
    }
    onClick(event);
  });
}

function getProofEditorApi(): Window['proof'] | null {
  if (typeof window === 'undefined') return null;
  return window.proof ?? null;
}

const TOP_FIXED_OVERLAY_IDS = ['share-banner', 'readonly-banner', 'review-lock-banner', 'error-banner'] as const;

function getTopViewportInset(margin: number): number {
  let inset = margin;
  for (const id of TOP_FIXED_OVERLAY_IDS) {
    const element = document.getElementById(id);
    if (!element) continue;
    const style = window.getComputedStyle(element);
    if (style.position !== 'fixed' && style.position !== 'sticky') continue;
    if (typeof element.getBoundingClientRect !== 'function') continue;
    const rect = element.getBoundingClientRect();
    if (rect.height <= 0 || rect.bottom <= 0) continue;
    inset = Math.max(inset, Math.ceil(rect.bottom + margin));
  }
  return inset;
}

function getAnchorBox(view: EditorView, anchor: MarkRange) {
  const from = view.coordsAtPos(anchor.from);
  const to = view.coordsAtPos(anchor.to);
  return {
    top: Math.min(from.top, to.top),
    bottom: Math.max(from.bottom, to.bottom),
    left: Math.min(from.left, to.left),
    right: Math.max(from.right, to.right)
  };
}

function positionPopover(element: HTMLElement, view: EditorView, anchor: MarkRange | null): void {
  if (!anchor) return;
  try {
    const anchorBox = getAnchorBox(view, anchor);
    if (typeof view.dom.getBoundingClientRect !== 'function') return;
    if (typeof element.getBoundingClientRect !== 'function') return;
    const editorRect = view.dom.getBoundingClientRect();
    const popoverRect = element.getBoundingClientRect();
    const margin = 12;
    const dockGap = 16;
    const viewportW = window.innerWidth;
    const viewportH = window.innerHeight;
    const safeTop = getTopViewportInset(margin);
    const maxTop = Math.max(safeTop, viewportH - popoverRect.height - margin);
    const spaceRight = viewportW - editorRect.right;
    const spaceLeft = editorRect.left;
    const canDockRight = spaceRight >= popoverRect.width + dockGap;
    const canDockLeft = spaceLeft >= popoverRect.width + dockGap;

    if (canDockRight || canDockLeft) {
      const dockRight = canDockRight || !canDockLeft;
      const left = dockRight
        ? clamp(editorRect.right + dockGap, margin, viewportW - popoverRect.width - margin)
        : clamp(editorRect.left - dockGap - popoverRect.width, margin, viewportW - popoverRect.width - margin);
      const top = clamp(anchorBox.top - 8, safeTop, maxTop);
      element.style.left = `${left}px`;
      element.style.top = `${top}px`;
      element.dataset.placement = dockRight ? 'side-right' : 'side-left';
      return;
    }

    const aboveTop = anchorBox.top - popoverRect.height - margin;
    const belowTop = anchorBox.bottom + margin;
    const hasRoomAbove = aboveTop >= safeTop;
    const hasRoomBelow = belowTop + popoverRect.height <= viewportH - margin;
    const top = hasRoomAbove
      ? aboveTop
      : (hasRoomBelow
        ? belowTop
        : clamp(anchorBox.top, safeTop, maxTop));
    const left = clamp(anchorBox.left, margin, viewportW - popoverRect.width - margin);
    element.style.left = `${left}px`;
    element.style.top = `${top}px`;
    element.dataset.placement = hasRoomAbove ? 'above' : 'below';
  } catch {
    // Ignore positioning errors for invalid positions.
  }
}

function shouldUseDesktopSuggestionPanel(): boolean {
  if (typeof window === 'undefined') return false;
  if (isMobileTouch()) return false;
  const coarsePointer = typeof window.matchMedia === 'function'
    ? window.matchMedia('(pointer: coarse)').matches
    : false;
  if (coarsePointer) return false;
  return window.innerWidth > 900;
}

function positionSidePanel(element: HTMLElement, view: EditorView, anchor: MarkRange | null): void {
  if (!anchor) return;
  try {
    const anchorBox = getAnchorBox(view, anchor);
    if (typeof view.dom.getBoundingClientRect !== 'function') return;
    if (typeof element.getBoundingClientRect !== 'function') return;
    const editorRect = view.dom.getBoundingClientRect();
    const panelRect = element.getBoundingClientRect();
    const margin = 16;
    const panelGap = 20;
    const viewportW = window.innerWidth;
    const viewportH = window.innerHeight;
    const safeTop = getTopViewportInset(margin);
    const maxTop = Math.max(safeTop, viewportH - panelRect.height - margin);
    const canDockRight = viewportW - editorRect.right >= panelRect.width + panelGap + margin;
    const canDockLeft = editorRect.left >= panelRect.width + panelGap + margin;

    let left = clamp(viewportW - panelRect.width - margin, margin, viewportW - panelRect.width - margin);
    let placement = 'panel-floating';
    if (canDockRight) {
      left = clamp(editorRect.right + panelGap, margin, viewportW - panelRect.width - margin);
      placement = 'panel-right';
    } else if (canDockLeft) {
      left = clamp(editorRect.left - panelGap - panelRect.width, margin, viewportW - panelRect.width - margin);
      placement = 'panel-left';
    }

    const top = clamp(anchorBox.top - 12, safeTop, maxTop);
    element.style.left = `${Math.round(left)}px`;
    element.style.top = `${Math.round(top)}px`;
    element.dataset.placement = placement;
  } catch {
    // Ignore positioning errors for invalid positions.
  }
}

function isResolvableComment(mark: Mark): boolean {
  if (mark.kind !== 'comment') return false;
  const data = mark.data as CommentData | undefined;
  return !Boolean(data?.resolved);
}

function isSuggestionKind(kind: string | undefined): kind is 'insert' | 'delete' | 'replace' {
  return kind === 'insert' || kind === 'delete' || kind === 'replace';
}

function getSuggestionKindPresentation(kind: 'insert' | 'delete' | 'replace'): {
  label: string;
  color: string;
  tint: string;
} {
  switch (kind) {
    case 'delete':
      return {
        label: 'Deletion',
        color: '#b91c1c',
        tint: 'rgba(185, 28, 28, 0.10)',
      };
    case 'replace':
      return {
        label: 'Replacement',
        color: '#1d4ed8',
        tint: 'rgba(29, 78, 216, 0.10)',
      };
    case 'insert':
    default:
      return {
        label: 'Insertion',
        color: '#15803d',
        tint: 'rgba(21, 128, 61, 0.10)',
      };
  }
}

class MarkPopoverController {
  private view: EditorView;
  private popover: HTMLDivElement;
  private backdrop: HTMLDivElement;
  private strip: HTMLDivElement;
  private suggestionRail: HTMLDivElement;
  private undoToast: HTMLDivElement;
  private mode: PopoverMode = null;
  private renderMode: RenderMode = 'legacy-popover';
  private threadFocusMode: ThreadFocusMode = 'reply-box';
  private activeMarkId: string | null = null;
  private anchor: MarkRange | null = null;
  private composeRange: MarkRange | null = null;
  private composeBy: string | null = null;
  private lastThreadLength: number = 0;
  private lastHandledPointerDownAt = 0;
  private undoMarkId: string | null = null;
  private undoTimer: number | null = null;
  private stripGestureStartX: number | null = null;
  private stripGestureCard: HTMLElement | null = null;
  private mobileStripRafScheduled: boolean = false;
  private mobileStripSignature: string = '';
  private mobileStripExpanded: boolean = false;
  private hasLiveSelection: boolean = false;
  private cachedActionRange: { range: MarkRange; text: string } | null = null;
  private cachedActionRangeAt: number = 0;
  private static ACTION_RANGE_CACHE_TTL_MS = 12_000;
  private handleSelectionChange: (() => void) | null = null;
  private handleEditorBlur: (() => void) | null = null;
  private blurPendingTimer: ReturnType<typeof setTimeout> | null = null;
  private selectionPollTimer: number | null = null;
  private selectionPollUntil: number = 0;
  private viewportSyncFramesRemaining: number = 0;
  private viewportSyncRaf: number | null = null;
  private mobileStripPaddingRaf: number | null = null;
  private mobileStripPaddingTarget: HTMLElement | null = null;
  private mobileStripPaddingOriginal: string | null = null;
  private mobileStripPaddingBase: number | null = null;
  private suggestionHoverOpenTimer: number | null = null;
  private suggestionHoverCloseTimer: number | null = null;
  private hoverPendingMarkId: string | null = null;
  private hoverPopoverActive: boolean = false;
  private activeSuggestionOpenedFromHover: boolean = false;
  private reviewActionRetryTimer: number | null = null;
  private suggestionRailSignature: string = '';

  private getAdjacentSuggestionMarkId(currentMarkId: string, direction: 'next' | 'prev'): string | null {
    const suggestions = getPendingSuggestions(getMarks(this.view.state))
      .sort((a, b) => (a.range?.from ?? 0) - (b.range?.from ?? 0));
    if (suggestions.length === 0) return null;

    const currentIndex = suggestions.findIndex((mark) => mark.id === currentMarkId);
    if (currentIndex < 0) {
      return direction === 'next'
        ? (suggestions[0]?.id ?? null)
        : (suggestions[suggestions.length - 1]?.id ?? null);
    }
    if (suggestions.length === 1) return null;

    const nextIndex = direction === 'next'
      ? (currentIndex + 1) % suggestions.length
      : (currentIndex - 1 + suggestions.length) % suggestions.length;
    const nextId = suggestions[nextIndex]?.id ?? null;
    return nextId === currentMarkId ? null : nextId;
  }

  private navigateToSuggestion(markId: string | null): void {
    if (!markId) return;
    this.clearReviewActionRetryTimer();
    const proof = getProofEditorApi();
    if (proof?.navigateToMark) {
      proof.navigateToMark(markId);
      return;
    }
    this.openForMark(markId);
  }

  private isSelectionWithinEditor(selection: Selection | null): boolean {
    if (!selection || selection.rangeCount === 0) return false;
    const withinNode = (node: Node | null): boolean => {
      if (!node) return false;
      const element = node.nodeType === Node.ELEMENT_NODE ? (node as Element) : node.parentElement;
      return Boolean(element && this.view.dom.contains(element));
    };
    return withinNode(selection.anchorNode) || withinNode(selection.focusNode);
  }

  private getDomSelectionRange(): { from: number; to: number } | null {
    const selection = document.getSelection();
    if (!selection || selection.rangeCount === 0) return null;
    const range = selection.getRangeAt(0);
    const startContainer = range.startContainer;
    const endContainer = range.endContainer;
    const getElement = (node: Node | null): Element | null => {
      if (!node) return null;
      return node.nodeType === Node.ELEMENT_NODE ? (node as Element) : node.parentElement;
    };
    const startElement = getElement(startContainer);
    const endElement = getElement(endContainer);
    if (!startElement || !endElement) return null;
    if (!this.view.dom.contains(startElement) || !this.view.dom.contains(endElement)) return null;
    try {
      const startPos = this.view.posAtDOM(startContainer, range.startOffset);
      const endPos = this.view.posAtDOM(endContainer, range.endOffset);
      const from = Math.min(startPos, endPos);
      const to = Math.max(startPos, endPos);
      return { from, to };
    } catch {
      return this.getDomSelectionRangeFromRects(range);
    }
  }

  private getDomSelectionRangeFromRects(range: Range): { from: number; to: number } | null {
    if (typeof range.getClientRects !== 'function') return null;
    const rects = Array.from(range.getClientRects());
    if (rects.length === 0) return null;
    const firstRect = rects[0];
    const lastRect = rects[rects.length - 1];
    const startPos = this.view.posAtCoords({
      left: Math.max(0, firstRect.left + 1),
      top: Math.max(0, firstRect.top + 1),
    })?.pos;
    const endPos = this.view.posAtCoords({
      left: Math.max(0, lastRect.right - 1),
      top: Math.max(0, lastRect.bottom - 1),
    })?.pos;
    if (typeof startPos !== 'number' || typeof endPos !== 'number') return null;
    const from = Math.min(startPos, endPos);
    const to = Math.max(startPos, endPos);
    if (from === to) return null;
    return { from, to };
  }

  private getProseMirrorSelectionRange(): MarkRange | null {
    const { from, to } = this.view.state.selection;
    if (from === to) return null;
    return { from, to };
  }

  private hasFreshCachedActionRange(): boolean {
    return Boolean(
      this.cachedActionRange
      && (Date.now() - this.cachedActionRangeAt < MarkPopoverController.ACTION_RANGE_CACHE_TTL_MS)
    );
  }

  private handleScroll = () => {
    if (this.mode && this.anchor && this.renderMode === 'legacy-popover') {
      positionPopover(this.popover, this.view, this.anchor);
    }
    this.renderSuggestionRail();
    if (shouldUseCommentUiV2()) {
      this.scheduleMobileStripRender();
    }
  };

  private handleViewportChange = () => {
    this.updateSheetViewportOffset();
    this.renderSuggestionRail();
    if (shouldUseCommentUiV2()) {
      this.scheduleMobileStripRender();
    }
    this.scheduleViewportSync();
  };

  private hideSuggestionRail(): void {
    this.suggestionRail.style.display = 'none';
    this.suggestionRail.innerHTML = '';
    this.suggestionRailSignature = '';
  }

  private renderSuggestionRail(): void {
    if (!this.view.dom.isConnected || isMobileTouch()) {
      this.hideSuggestionRail();
      return;
    }

    if (getSuggestionDisplayMode(this.view.state) !== 'simple') {
      this.hideSuggestionRail();
      return;
    }

    const pendingSuggestions = getPendingSuggestions(getMarks(this.view.state))
      .filter((mark) => isSuggestionKind(mark.kind));
    if (pendingSuggestions.length === 0) {
      this.hideSuggestionRail();
      return;
    }

    let editorRect: DOMRect;
    try {
      editorRect = this.view.dom.getBoundingClientRect();
    } catch {
      this.hideSuggestionRail();
      return;
    }

    if (editorRect.height <= 0 || editorRect.width <= 0) {
      this.hideSuggestionRail();
      return;
    }

    type SuggestionRailItem = {
      top: number;
      markIds: string[];
      active: boolean;
      kind: 'insert' | 'delete' | 'replace';
    };

    const grouped = new Map<string, SuggestionRailItem>();
    for (const mark of pendingSuggestions) {
      const anchor = resolveAnchorRange(this.view, mark);
      if (!anchor) continue;

      try {
        const coords = this.view.coordsAtPos(anchor.from);
        const relativeTop = clamp(
          Math.round(coords.top - editorRect.top),
          0,
          Math.max(0, Math.round(editorRect.height - 20)),
        );
        const key = String(relativeTop);
        const existing = grouped.get(key);
        if (existing) {
          existing.markIds.push(mark.id);
          existing.active = existing.active || mark.id === this.activeMarkId;
          if (existing.kind !== 'replace' && mark.kind === 'replace') {
            existing.kind = mark.kind;
          }
          continue;
        }
        grouped.set(key, {
          top: relativeTop,
          markIds: [mark.id],
          active: mark.id === this.activeMarkId,
          kind: mark.kind,
        });
      } catch {
        // Ignore marks whose current position can no longer be resolved visually.
      }
    }

    const items = Array.from(grouped.values())
      .sort((a, b) => a.top - b.top);

    if (items.length === 0) {
      this.hideSuggestionRail();
      return;
    }

    const railWidth = 24;
    const railLeft = clamp(
      Math.round(editorRect.right + 12),
      8,
      Math.max(8, Math.round(window.innerWidth - railWidth - 8)),
    );
    this.suggestionRail.style.cssText = [
      'position: fixed',
      `left: ${railLeft}px`,
      `top: ${Math.round(editorRect.top)}px`,
      `width: ${railWidth}px`,
      `height: ${Math.round(editorRect.height)}px`,
      'pointer-events: none',
      'z-index: 120',
      'display: block',
    ].join(';');

    const signature = JSON.stringify({
      railLeft,
      railTop: Math.round(editorRect.top),
      railHeight: Math.round(editorRect.height),
      items: items.map((item) => ({
        top: item.top,
        ids: item.markIds,
        active: item.active,
        kind: item.kind,
      })),
    });
    if (signature === this.suggestionRailSignature) {
      return;
    }

    this.suggestionRailSignature = signature;
    this.suggestionRail.innerHTML = '';

    for (const item of items) {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'mark-suggestion-rail-button';
      button.dataset.markId = item.markIds[0] ?? '';
      button.dataset.markIds = item.markIds.join(',');
      button.dataset.markKind = item.kind;

      const { color: baseColor, tint: baseTint } = getSuggestionKindPresentation(item.kind);
      const borderColor = item.active ? '#111827' : baseColor;
      const background = item.active
        ? baseColor
        : baseTint;

      button.style.cssText = [
        'position: absolute',
        'right: 2px',
        `top: ${Math.max(0, item.top - 2)}px`,
        'width: 18px',
        `height: ${item.markIds.length > 1 ? 24 : 20}px`,
        'border-radius: 999px',
        `border: 1px solid ${borderColor}`,
        `background: ${background}`,
        'box-sizing: border-box',
        'pointer-events: auto',
        'cursor: pointer',
        'padding: 0',
        'display: inline-flex',
        'align-items: center',
        'justify-content: center',
        'font: 600 10px/1 system-ui, sans-serif',
        `color: ${item.active ? '#ffffff' : baseColor}`,
        'box-shadow: 0 1px 2px rgba(15, 23, 42, 0.12)',
      ].join(';');

      if (item.markIds.length > 1) {
        button.textContent = String(item.markIds.length);
      }

      const changeLabel = item.markIds.length === 1 ? 'change' : 'changes';
      button.title = `${item.markIds.length} pending ${changeLabel} on this line`;
      button.setAttribute('aria-label', button.title);

      button.addEventListener('pointerdown', (event) => {
        event.preventDefault();
        event.stopPropagation();
      });
      button.addEventListener('mousedown', (event) => {
        event.preventDefault();
        event.stopPropagation();
      });
      button.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        const currentId = this.activeMarkId;
        const currentIndex = currentId ? item.markIds.indexOf(currentId) : -1;
        const nextMarkId = currentIndex >= 0
          ? item.markIds[(currentIndex + 1) % item.markIds.length] ?? item.markIds[0] ?? null
          : item.markIds[0] ?? null;
        if (!nextMarkId) return;
        this.openForMark(nextMarkId, undefined, { source: 'direct' });
      });

      this.suggestionRail.appendChild(button);
    }
  }

  private handleEditorTouchStart = () => {
    if (!shouldUseCommentUiV2()) return;
    this.scheduleSelectionPolling();
  };

  private handleEditorTouchEnd = () => {
    if (!shouldUseCommentUiV2()) return;
    this.scheduleSelectionPolling();
  };

  private scheduleSelectionPolling(durationMs: number = MOBILE_SELECTION_POLL_WINDOW_MS): void {
    if (!isMobileTouch()) return;
    this.selectionPollUntil = Math.max(this.selectionPollUntil, Date.now() + durationMs);
    if (this.selectionPollTimer !== null) return;
    this.selectionPollTimer = window.setInterval(() => {
      if (this.mode !== null || Date.now() >= this.selectionPollUntil) {
        this.stopSelectionPolling();
        return;
      }
      this.handleSelectionChange?.();
    }, MOBILE_SELECTION_POLL_MS);
  }

  private stopSelectionPolling(): void {
    if (this.selectionPollTimer !== null) {
      window.clearInterval(this.selectionPollTimer);
      this.selectionPollTimer = null;
    }
    this.selectionPollUntil = 0;
  }

  private handleEditorPointerDown = (event: PointerEvent) => {
    if (!shouldUseCommentUiV2()) return;
    if (event.pointerType === 'touch') {
      this.scheduleSelectionPolling();
    }
    const target = event.target as HTMLElement | null;
    const markEl = target?.closest('[data-mark-id]') as HTMLElement | null;
    if (!markEl) return;
    const markId = markEl.dataset.markId;
    if (!markId) return;
    if (event.pointerType === 'touch') {
      event.preventDefault();
    }
    event.stopPropagation();
    this.lastHandledPointerDownAt = Date.now();
    const coords = { left: event.clientX, top: event.clientY };
    const pos = this.view.posAtCoords(coords)?.pos ?? null;
    this.openForMark(markId, pos);
  };

  private handleEditorClick = (event: MouseEvent) => {
    if ((Date.now() - this.lastHandledPointerDownAt) < 450) return;
    const target = event.target as HTMLElement | null;
    const markEl = target?.closest('[data-mark-id]') as HTMLElement | null;
    if (!markEl) return;
    const markId = markEl.dataset.markId;
    if (!markId) return;
    event.stopPropagation();
    const coords = { left: event.clientX, top: event.clientY };
    const pos = this.view.posAtCoords(coords)?.pos ?? null;
    this.openForMark(markId, pos, { source: 'direct' });
  };

  private clearSuggestionHoverTimers(): void {
    if (this.suggestionHoverOpenTimer !== null) {
      window.clearTimeout(this.suggestionHoverOpenTimer);
      this.suggestionHoverOpenTimer = null;
    }
    if (this.suggestionHoverCloseTimer !== null) {
      window.clearTimeout(this.suggestionHoverCloseTimer);
      this.suggestionHoverCloseTimer = null;
    }
    this.hoverPendingMarkId = null;
  }

  private clearReviewActionRetryTimer(): void {
    if (this.reviewActionRetryTimer !== null) {
      window.clearTimeout(this.reviewActionRetryTimer);
      this.reviewActionRetryTimer = null;
    }
  }

  private runSuggestionReviewAction(
    markId: string,
    action: 'accept' | 'reject',
    nextMarkId: string | null,
  ): void {
    this.clearReviewActionRetryTimer();

    const setReviewButtonsBusy = (busy: boolean): void => {
      const buttons = Array.from(this.popover.querySelectorAll('.mark-popover-actions button')) as HTMLButtonElement[];
      for (const button of buttons) {
        if (busy) {
          button.dataset.reviewBusyDisabled = button.disabled ? 'true' : 'false';
          button.disabled = true;
        } else {
          button.disabled = button.dataset.reviewBusyDisabled === 'true';
          delete button.dataset.reviewBusyDisabled;
        }
      }
      this.popover.setAttribute('aria-busy', busy ? 'true' : 'false');
    };

    const runAction = (): boolean => {
      const proof = getProofEditorApi();
      if (action === 'accept') {
        if (proof?.markAccept) {
          return proof.markAccept(markId);
        }
        return acceptSuggestion(this.view, markId);
      }
      if (proof?.markReject) {
        return proof.markReject(markId);
      }
      return rejectSuggestion(this.view, markId);
    };

    const runLocalActionOnly = (): boolean => {
      if (action === 'accept') {
        return acceptSuggestion(this.view, markId);
      }
      return false;
    };

    const finish = (): void => {
      this.clearReviewActionRetryTimer();
      if (nextMarkId) {
        this.navigateToSuggestion(nextMarkId);
      } else {
        this.close();
      }
    };

    const proof = getProofEditorApi();
    const persistedAction = action === 'accept'
      ? (typeof proof?.markAcceptPersisted === 'function'
        ? () => proof.markAcceptPersisted(markId)
        : null)
      : (typeof proof?.markRejectPersisted === 'function'
        ? () => proof.markRejectPersisted(markId)
        : null);
    if (persistedAction) {
      const optimisticApplied = runLocalActionOnly();
      if (optimisticApplied) {
        finish();
      } else {
        setReviewButtonsBusy(true);
      }
      void persistedAction().then((success) => {
        if (!success) {
          if (!optimisticApplied) {
            setReviewButtonsBusy(false);
          }
          return;
        }
        if (!optimisticApplied) {
          finish();
        }
      }).catch((error) => {
        console.error('[mark-popover] Persisted review action failed:', error);
        if (!optimisticApplied) {
          setReviewButtonsBusy(false);
        }
      });
      return;
    }

    const attempt = (remainingRetries: number): void => {
      const markStillExists = getMarks(this.view.state).some((item) => item.id === markId);
      if (!markStillExists) {
        finish();
        return;
      }

      if (runAction()) {
        finish();
        return;
      }

      if (remainingRetries <= 0) return;

      this.reviewActionRetryTimer = window.setTimeout(() => {
        this.reviewActionRetryTimer = null;
        attempt(remainingRetries - 1);
      }, REVIEW_ACTION_RETRY_DELAY_MS);
    };

    attempt(REVIEW_ACTION_MAX_RETRIES);
  }

  private scheduleSuggestionHoverClose(): void {
    if (isMobileTouch()) return;
    if (this.renderMode === 'desktop-side-panel') return;
    if (!this.activeSuggestionOpenedFromHover) return;
    if (this.suggestionHoverCloseTimer !== null) {
      window.clearTimeout(this.suggestionHoverCloseTimer);
    }
    this.suggestionHoverCloseTimer = window.setTimeout(() => {
      this.suggestionHoverCloseTimer = null;
      if (this.hoverPopoverActive) return;
      if (this.activeSuggestionOpenedFromHover && this.mode === 'suggestion') {
        this.close();
      }
    }, SUGGESTION_HOVER_CLOSE_DELAY_MS);
  }

  private handleEditorMouseMove = (event: MouseEvent) => {
    if (isMobileTouch()) return;
    if (shouldUseDesktopSuggestionPanel()) return;
    if (event.buttons !== 0) return;
    const target = event.target as HTMLElement | null;
    const markEl = target?.closest('[data-mark-id]') as HTMLElement | null;
    const markId = markEl?.dataset.markId ?? null;
    const markKind = markEl?.dataset.markKind;
    if (!markId || !isSuggestionKind(markKind)) {
      this.scheduleSuggestionHoverClose();
      return;
    }

    if (this.suggestionHoverCloseTimer !== null) {
      window.clearTimeout(this.suggestionHoverCloseTimer);
      this.suggestionHoverCloseTimer = null;
    }

    if (this.mode === 'suggestion' && this.activeMarkId === markId) {
      return;
    }

    if (this.hoverPendingMarkId === markId) {
      return;
    }

    if (this.suggestionHoverOpenTimer !== null) {
      window.clearTimeout(this.suggestionHoverOpenTimer);
    }

    this.hoverPendingMarkId = markId;
    const hoverClientX = event.clientX;
    const hoverClientY = event.clientY;
    this.suggestionHoverOpenTimer = window.setTimeout(() => {
      this.suggestionHoverOpenTimer = null;
      const pendingMarkId = this.hoverPendingMarkId;
      this.hoverPendingMarkId = null;
      if (!pendingMarkId) return;
      const coords = { left: hoverClientX, top: hoverClientY };
      const pos = this.view.posAtCoords(coords)?.pos ?? null;
      this.openForMark(pendingMarkId, pos, { source: 'hover' });
      if (this.mode === 'suggestion' && this.activeMarkId === pendingMarkId && this.popover.style.display === 'none') {
        const mark = getMarks(this.view.state).find(item => item.id === pendingMarkId);
        if (mark) {
          this.renderSuggestion(mark);
          this.open();
        }
      }
    }, SUGGESTION_HOVER_OPEN_DELAY_MS);
  };

  private handleEditorMouseLeave = () => {
    this.scheduleSuggestionHoverClose();
  };

  private handleOutsidePointerDown = (event: PointerEvent) => {
    const target = event.target as Node;
    if (this.popover.contains(target)) return;
    if (this.strip.contains(target)) return;
    this.close();
  };

  private handleOutsideClick = (event: MouseEvent) => {
    if ((Date.now() - this.lastHandledPointerDownAt) < 450) return;
    const target = event.target as Node;
    if (this.popover.contains(target)) return;
    if (this.strip.contains(target)) return;
    this.close();
  };

  private handleKeydown = (event: KeyboardEvent) => {
    if (event.key === 'Escape') {
      this.close();
      return;
    }

    if (event.defaultPrevented || this.mode !== 'suggestion' || !this.activeMarkId) return;

    const mark = getMarks(this.view.state).find(item => item.id === this.activeMarkId);
    if (!mark || (mark.kind !== 'insert' && mark.kind !== 'delete' && mark.kind !== 'replace')) {
      return;
    }

    const nextMarkId = this.getAdjacentSuggestionMarkId(mark.id, 'next');
    const previousMarkId = this.getAdjacentSuggestionMarkId(mark.id, 'prev');

    if (matchesReviewShortcut(event, { key: 'a', code: 'KeyA' })) {
      event.preventDefault();
      event.stopPropagation();
      this.runSuggestionReviewAction(mark.id, 'accept', nextMarkId);
      return;
    }

    if (matchesReviewShortcut(event, { key: 'r', code: 'KeyR' })) {
      event.preventDefault();
      event.stopPropagation();
      this.runSuggestionReviewAction(mark.id, 'reject', nextMarkId);
      return;
    }

    if (matchesReviewShortcut(event, { key: ']', code: 'BracketRight' })) {
      if (!nextMarkId) return;
      event.preventDefault();
      event.stopPropagation();
      this.navigateToSuggestion(nextMarkId);
      return;
    }

    if (matchesReviewShortcut(event, { key: '[', code: 'BracketLeft' })) {
      if (!previousMarkId) return;
      event.preventDefault();
      event.stopPropagation();
      this.navigateToSuggestion(previousMarkId);
    }
  };

  constructor(view: EditorView) {
    this.view = view;

    this.popover = document.createElement('div');
    this.popover.className = 'mark-popover';
    this.popover.tabIndex = -1;
    this.popover.style.display = 'none';
    this.popover.addEventListener('pointerdown', event => {
      event.stopPropagation();
    });
    this.popover.addEventListener('mouseenter', () => {
      this.hoverPopoverActive = true;
      if (this.suggestionHoverCloseTimer !== null) {
        window.clearTimeout(this.suggestionHoverCloseTimer);
        this.suggestionHoverCloseTimer = null;
      }
    });
    this.popover.addEventListener('mouseleave', () => {
      this.hoverPopoverActive = false;
      this.scheduleSuggestionHoverClose();
    });

    this.backdrop = document.createElement('div');
    this.backdrop.className = 'mark-popover-backdrop';
    this.backdrop.style.display = 'none';
    this.backdrop.addEventListener('pointerdown', (event) => {
      if (event.pointerType === 'touch') {
        event.preventDefault();
      }
      event.stopPropagation();
      this.close();
    });

    this.strip = document.createElement('div');
    this.strip.className = 'mark-mobile-strip';
    this.strip.style.display = 'none';
    this.strip.addEventListener('pointerdown', (event) => {
      const card = (event.target as HTMLElement | null)?.closest('[data-mark-id]') as HTMLElement | null;
      if (!card) return;
      this.stripGestureStartX = event.clientX;
      this.stripGestureCard = card;
      this.stripGestureCard.style.transition = '';
    });
    this.strip.addEventListener('pointermove', (event) => {
      if (this.stripGestureStartX === null || !this.stripGestureCard) return;
      const deltaX = event.clientX - this.stripGestureStartX;
      const translateX = Math.max(-96, Math.min(0, deltaX));
      this.stripGestureCard.style.transform = `translateX(${translateX}px)`;
      this.stripGestureCard.style.opacity = `${Math.max(0.58, 1 + (translateX / 220))}`;
    });
    this.strip.addEventListener('pointerup', (event) => {
      const startX = this.stripGestureStartX;
      const card = this.stripGestureCard;
      this.resetStripGestureVisual();
      if (startX === null) return;
      const deltaX = event.clientX - startX;
      if (!card) return;
      const markId = card.dataset.markId;
      if (!markId) return;
      if (deltaX <= -60) {
        this.resolveFromStrip(markId);
      }
    });
    this.strip.addEventListener('pointercancel', () => {
      this.resetStripGestureVisual();
    });
    this.strip.addEventListener('pointerleave', (event) => {
      if ((event.buttons & 1) === 0) {
        this.resetStripGestureVisual();
      }
    });

    this.undoToast = document.createElement('div');
    this.undoToast.className = 'mark-mobile-undo';
    this.undoToast.style.display = 'none';

    this.suggestionRail = document.createElement('div');
    this.suggestionRail.className = 'mark-suggestion-rail';
    this.suggestionRail.style.display = 'none';
    this.suggestionRail.addEventListener('pointerdown', (event) => {
      event.stopPropagation();
    });
    this.suggestionRail.addEventListener('mousedown', (event) => {
      event.stopPropagation();
    });
    this.suggestionRail.addEventListener('click', (event) => {
      event.stopPropagation();
    });

    document.body.appendChild(this.backdrop);
    document.body.appendChild(this.popover);
    document.body.appendChild(this.strip);
    document.body.appendChild(this.undoToast);
    document.body.appendChild(this.suggestionRail);

    view.dom.addEventListener('pointerdown', this.handleEditorPointerDown);
    view.dom.addEventListener('click', this.handleEditorClick);
    view.dom.addEventListener('mousemove', this.handleEditorMouseMove);
    view.dom.addEventListener('mouseleave', this.handleEditorMouseLeave);
    view.dom.addEventListener('touchstart', this.handleEditorTouchStart, { passive: true });
    view.dom.addEventListener('touchend', this.handleEditorTouchEnd);
    window.addEventListener('scroll', this.handleScroll);
    window.addEventListener('resize', this.handleScroll);
    window.visualViewport?.addEventListener('resize', this.handleViewportChange);
    window.visualViewport?.addEventListener('scroll', this.handleViewportChange);

    this.handleSelectionChange = () => {
      const mobile = isMobileTouch();
      const selection = document.getSelection();
      const domRange = mobile ? this.getDomSelectionRange() : null;
      const pmRange = this.getProseMirrorSelectionRange();
      const mobileRange = mobile ? (domRange ?? pmRange) : null;
      const hasDomSelectionInsideEditor = mobile
        ? this.isSelectionWithinEditor(selection)
        : false;
      const hasDomSelectionText = mobile
        ? Boolean(selection && selection.toString().trim().length > 0 && hasDomSelectionInsideEditor)
        : false;
      const withinEditor = mobile
        ? (Boolean(mobileRange) || hasDomSelectionInsideEditor)
        : this.isSelectionWithinEditor(selection);
      const hasFocus = this.view.hasFocus();
      const domContains = this.view.dom.contains(document.activeElement);
      const { from: pmFrom, to: pmTo } = this.view.state.selection;
      const selectionFrom = mobile ? mobileRange?.from ?? null : pmFrom;
      const selectionTo = mobile ? mobileRange?.to ?? null : pmTo;
      const hasFreshCache = this.hasFreshCachedActionRange();

      if (this.mode !== null) return;

      if (mobile) {
        // During iOS blur/callout timing windows DOM selection mapping can
        // transiently fail. If we still have no usable range, preserve state.
        if (this.blurPendingTimer && !mobileRange) return;
        if (!withinEditor) {
          const selectionCleared = !selection || selection.rangeCount === 0;
          if (selectionCleared && this.hasLiveSelection && !this.blurPendingTimer) {
            this.hasLiveSelection = false;
            this.mobileStripSignature = '';
            this.scheduleMobileStripRender(true);
            return;
          }
          if (this.hasLiveSelection && !hasFreshCache) {
            this.hasLiveSelection = false;
            this.mobileStripSignature = '';
            this.scheduleMobileStripRender(true);
          }
          return;
        }
      } else {
        if (!hasFocus) return;
        if (!domContains) return;
      }

      try {
        if (selectionFrom !== null && selectionTo !== null && selectionFrom !== selectionTo) {
          const nextRange = { from: selectionFrom, to: selectionTo };
          const cached = this.cachedActionRange?.range;
          const changed = !cached || cached.from !== nextRange.from || cached.to !== nextRange.to;
          this.hasLiveSelection = true;
          if (changed) {
            const text = this.view.state.doc.textBetween(selectionFrom, selectionTo);
            this.cachedActionRange = { range: nextRange, text };
            this.cachedActionRangeAt = Date.now();
            this.mobileStripSignature = '';
          }
          if (this.mobileStripExpanded) {
            this.mobileStripExpanded = false;
          }
        } else {
          if (mobile && hasDomSelectionText) {
            // Keep action row visible while iOS selection mapping catches up.
            this.hasLiveSelection = true;
          } else if (!(mobile && hasFreshCache)) {
            this.hasLiveSelection = false;
          }
        }
        this.mobileStripSignature = '';
        // Render immediately on mobile — RAF timing races with ProseMirror
        // update cycles that can reset hasLiveSelection before render fires.
        this.scheduleMobileStripRender(mobile);
        if (mobile && hasDomSelectionText) {
          this.scheduleSelectionPolling();
        }
      } catch {
        // Selection range may be invalid after collab edits — silently ignore
      }
    };
    document.addEventListener('selectionchange', this.handleSelectionChange);

    // Clear action row when editor loses focus
    this.handleEditorBlur = () => {
      if (isMobileTouch()) {
        // Use a pending timer so selectionchange doesn't re-set hasLiveSelection
        if (this.blurPendingTimer) clearTimeout(this.blurPendingTimer);
        this.blurPendingTimer = setTimeout(() => {
          this.blurPendingTimer = null;
          const selection = document.getSelection();
          const domRange = this.getDomSelectionRange();
          const pmRange = this.getProseMirrorSelectionRange();
          const effectiveRange = domRange ?? pmRange;
          const selectionInsideEditor = this.isSelectionWithinEditor(selection);
          const hasSelectionText = Boolean(
            selection && selection.toString().trim().length > 0 && selectionInsideEditor
          );
          // If editor regained focus (e.g., user tapped an action button
          // which briefly blurs then refocuses), don't clear
          if (this.view.hasFocus() || this.view.dom.contains(document.activeElement)) return;
          if (effectiveRange && effectiveRange.from !== effectiveRange.to) {
            const text = this.view.state.doc.textBetween(effectiveRange.from, effectiveRange.to);
            this.cachedActionRange = { range: effectiveRange, text };
            this.cachedActionRangeAt = Date.now();
            this.hasLiveSelection = true;
            this.mobileStripSignature = '';
            this.scheduleMobileStripRender(true);
            return;
          }
          if (hasSelectionText) {
            this.hasLiveSelection = true;
            this.mobileStripSignature = '';
            this.scheduleMobileStripRender(true);
            this.scheduleSelectionPolling();
            return;
          }
          if (this.hasLiveSelection) {
            this.hasLiveSelection = false;
            this.mobileStripSignature = '';
            this.scheduleMobileStripRender(true);
          }
        }, 150);
        return;
      }
      if (this.hasLiveSelection) {
        this.hasLiveSelection = false;
        this.mobileStripSignature = '';
        this.scheduleMobileStripRender();
      }
    };
    this.view.dom.addEventListener('blur', this.handleEditorBlur);
  }

  destroy(): void {
    this.close();
    this.clearUndoToast();
    this.resetStripGestureVisual();
    this.clearSuggestionHoverTimers();
    this.view.dom.removeEventListener('pointerdown', this.handleEditorPointerDown);
    this.view.dom.removeEventListener('click', this.handleEditorClick);
    this.view.dom.removeEventListener('mousemove', this.handleEditorMouseMove);
    this.view.dom.removeEventListener('mouseleave', this.handleEditorMouseLeave);
    this.view.dom.removeEventListener('touchstart', this.handleEditorTouchStart);
    this.view.dom.removeEventListener('touchend', this.handleEditorTouchEnd);
    window.removeEventListener('scroll', this.handleScroll);
    window.removeEventListener('resize', this.handleScroll);
    window.visualViewport?.removeEventListener('resize', this.handleViewportChange);
    window.visualViewport?.removeEventListener('scroll', this.handleViewportChange);
    if (this.handleSelectionChange) {
      document.removeEventListener('selectionchange', this.handleSelectionChange);
      this.handleSelectionChange = null;
    }
    if (this.handleEditorBlur) {
      this.view.dom.removeEventListener('blur', this.handleEditorBlur);
      this.handleEditorBlur = null;
    }
    if (this.blurPendingTimer) {
      clearTimeout(this.blurPendingTimer);
      this.blurPendingTimer = null;
    }
    this.stopSelectionPolling();
    this.clearReviewActionRetryTimer();
    if (this.viewportSyncRaf !== null) {
      cancelAnimationFrame(this.viewportSyncRaf);
      this.viewportSyncRaf = null;
      this.viewportSyncFramesRemaining = 0;
    }
    if (this.mobileStripPaddingRaf !== null) {
      cancelAnimationFrame(this.mobileStripPaddingRaf);
      this.mobileStripPaddingRaf = null;
    }
    this.popover.remove();
    this.backdrop.remove();
    this.strip.remove();
    this.undoToast.remove();
    this.suggestionRail.remove();
    this.clearMobileStripPadding();
  }

  update(view: EditorView): void {
    this.view = view;

    const nextRenderMode = this.getPreferredRenderMode(this.mode);
    if (this.mode && nextRenderMode !== this.renderMode) {
      if (this.mode === 'composer') {
        this.renderMode = nextRenderMode;
        this.renderComposer();
      } else if (this.activeMarkId) {
        this.renderMode = nextRenderMode;
        this.openForMark(this.activeMarkId, undefined, { threadFocusMode: this.threadFocusMode });
      }
    }

    if (this.mode === 'suggestion') {
      const stateActiveMarkId = getActiveMarkId(view.state);
      if (stateActiveMarkId && stateActiveMarkId !== this.activeMarkId) {
        this.openForMark(stateActiveMarkId, undefined, {
          source: this.activeSuggestionOpenedFromHover ? 'hover' : 'direct',
        });
        this.scheduleMobileStripRender();
        return;
      }

      if (this.activeMarkId) {
        const marks = getMarks(view.state);
        const mark = marks.find(item => item.id === this.activeMarkId);
        if (!mark) {
          this.close();
          return;
        }
        this.renderSuggestion(mark);
        if (this.popover.style.display === 'none') {
          this.open();
        }
      }
    }

    if (this.mode && this.anchor) {
      if (this.renderMode === 'legacy-popover') {
        positionPopover(this.popover, view, this.anchor);
      } else if (this.renderMode === 'desktop-side-panel') {
        positionSidePanel(this.popover, view, this.anchor);
      }
      this.updateSheetViewportOffset();
    }

    if (this.mode === 'thread' && this.activeMarkId) {
      const marks = getMarks(view.state);
      const mark = marks.find(item => item.id === this.activeMarkId);
      if (mark) {
        const data = mark.data as CommentData | undefined;
        const threadId = data?.thread;
        if (threadId) {
          const thread = getThread(marks, threadId);
          if (thread.length !== this.lastThreadLength) {
            this.lastThreadLength = thread.length;
            this.renderThread(mark);
          }
        }
      }
    }

    this.renderSuggestionRail();
    this.scheduleMobileStripRender();
  }

  openComposer(range: MarkRange, by: string): void {
    if (!canCommentInRuntime()) return;
    this.mode = 'composer';
    this.composeRange = range;
    this.composeBy = by;
    this.activeMarkId = null;
    this.anchor = range;
    this.mobileStripExpanded = false;
    this.hasLiveSelection = false;
    this.cachedActionRange = null;
    this.cachedActionRangeAt = 0;
    this.renderMode = this.getPreferredRenderMode('composer');

    setActiveMark(this.view, null);
    setComposeAnchorRange(this.view, range);
    this.ensureAnchorVisible();
    this.renderComposer();
    this.open();
  }

  openForMark(
    markId: string,
    pos?: number | null,
    options?: { threadFocusMode?: ThreadFocusMode; source?: 'direct' | 'hover' },
  ): void {
    this.clearReviewActionRetryTimer();
    const marks = getMarks(this.view.state);
    const mark = marks.find(item => item.id === markId);
    if (!mark) return;

    this.mode = mark.kind === 'comment' ? 'thread' : 'suggestion';
    this.activeMarkId = markId;
    this.anchor = resolveAnchorRange(this.view, mark, pos);
    this.composeRange = null;
    this.composeBy = null;
    this.mobileStripExpanded = false;
    this.hasLiveSelection = false;
    this.cachedActionRange = null;
    this.cachedActionRangeAt = 0;
    this.renderMode = this.getPreferredRenderMode(this.mode);
    this.threadFocusMode = options?.threadFocusMode ?? 'reply-box';
    this.activeSuggestionOpenedFromHover = mark.kind !== 'comment' && options?.source === 'hover';
    if (this.suggestionHoverCloseTimer !== null) {
      window.clearTimeout(this.suggestionHoverCloseTimer);
      this.suggestionHoverCloseTimer = null;
    }

    setActiveMark(this.view, markId);
    setComposeAnchorRange(this.view, null);
    this.ensureAnchorVisible();

    if (mark.kind === 'comment') {
      this.renderThread(mark);
    } else {
      this.renderSuggestion(mark);
    }
    this.open();
  }

  close(): void {
    if (this.mode === null) {
      this.hideOverlayChrome();
      return;
    }

    this.mode = null;
    this.activeMarkId = null;
    this.composeRange = null;
    this.composeBy = null;
    this.anchor = null;
    this.lastThreadLength = 0;
    this.threadFocusMode = 'reply-box';
    this.mobileStripSignature = '';
    this.mobileStripExpanded = false;
    this.hasLiveSelection = false;
    this.cachedActionRange = null;
    this.cachedActionRangeAt = 0;
    this.activeSuggestionOpenedFromHover = false;
    this.hoverPopoverActive = false;
    this.clearReviewActionRetryTimer();

    this.hideOverlayChrome();

    this.popover.style.display = 'none';
    this.popover.innerHTML = '';
    setActiveMark(this.view, null);
    setComposeAnchorRange(this.view, null);
    this.scheduleMobileStripRender();

    document.removeEventListener('pointerdown', this.handleOutsidePointerDown);
    document.removeEventListener('mousedown', this.handleOutsideClick);
    document.removeEventListener('keydown', this.handleKeydown, true);
    // Defer focus restoration so that outside clicks can land on their
    // intended target first. If after a frame the focus is still on body
    // or inside the now-hidden popover/strip/backdrop, restore to editor.
    requestAnimationFrame(() => {
      const active = document.activeElement;
      const shouldRestoreFocus = !active
        || active === document.body
        || this.popover.contains(active)
        || this.strip.contains(active)
        || this.backdrop.contains(active);
      if (shouldRestoreFocus) {
        this.view.focus();
      }
    });
  }

  private open(): void {
    this.popover.style.display = 'block';
    this.popover.classList.toggle('mark-popover-side-panel', this.renderMode === 'desktop-side-panel');
    if (this.renderMode === 'legacy-popover') {
      this.popover.classList.remove('mark-popover-sheet');
      this.backdrop.style.display = 'none';
      positionPopover(this.popover, this.view, this.anchor);
    } else if (this.renderMode === 'desktop-side-panel') {
      this.popover.classList.remove('mark-popover-sheet');
      this.backdrop.style.display = 'none';
      positionSidePanel(this.popover, this.view, this.anchor);
    } else {
      this.popover.classList.add('mark-popover-sheet');
      this.backdrop.style.display = 'block';
      this.updateSheetViewportOffset();
      if (this.mode === 'composer' || this.mode === 'thread') {
        const textarea = this.popover.querySelector('.mark-popover-textarea') as HTMLTextAreaElement | null;
        if (textarea) {
          try {
            textarea.focus({ preventScroll: true });
          } catch {
            textarea.focus();
          }
          this.updateSheetViewportOffset();
        } else {
          this.focusSheetContainer();
        }
      } else {
        this.focusSheetContainer();
      }
    }
    if (shouldUseCommentUiV2()) {
      this.strip.style.display = 'none';
      this.strip.classList.remove('mark-mobile-strip-expanded');
    }
    document.addEventListener('pointerdown', this.handleOutsidePointerDown);
    document.addEventListener('mousedown', this.handleOutsideClick);
    document.addEventListener('keydown', this.handleKeydown, true);
  }

  private hideOverlayChrome(): void {
    this.backdrop.style.display = 'none';
    this.popover.classList.remove('mark-popover-sheet');
    this.popover.classList.remove('mark-popover-side-panel');
    this.popover.classList.remove('mark-popover-keyboard-open');
    this.popover.style.bottom = '';
    this.popover.style.maxHeight = '';
  }

  private getPreferredRenderMode(mode: PopoverMode): RenderMode {
    if (mode === 'suggestion' && shouldUseDesktopSuggestionPanel()) {
      return 'desktop-side-panel';
    }
    return shouldUseCommentUiV2() ? 'mobile-sheet' : 'legacy-popover';
  }

  private updateSheetViewportOffset(): void {
    if (!shouldUseCommentUiV2()) return;
    const vv = window.visualViewport;
    const offset = getViewportOffset(window.innerHeight, vv ?? null);
    const viewportHeight = getVisualViewportHeight(window.innerHeight, vv ?? null);
    const safeTop = getTopViewportInset(12);
    const maxHeight = Math.max(220, Math.min(560, Math.floor(viewportHeight - safeTop - 12)));

    if (this.renderMode === 'mobile-sheet' && this.mode) {
      this.popover.style.bottom = `${offset}px`;
      this.popover.style.maxHeight = `${maxHeight}px`;
      this.popover.classList.toggle('mark-popover-keyboard-open', offset > 0);
    }
    const sabValue = getComputedStyle(document.documentElement).getPropertyValue('--sab');
    const safeBottom = parseInt(sabValue, 10) || 0;
    this.strip.style.bottom = `${offset + 12 + safeBottom}px`;
    this.undoToast.style.bottom = `${offset + 80}px`;
  }

  private scheduleViewportSync(frames: number = VIEWPORT_SYNC_FRAMES): void {
    if (this.renderMode !== 'mobile-sheet') return;
    this.viewportSyncFramesRemaining = Math.max(this.viewportSyncFramesRemaining, frames);
    if (this.viewportSyncRaf !== null) return;
    const step = () => {
      this.viewportSyncRaf = null;
      if (this.viewportSyncFramesRemaining <= 0) return;
      this.updateSheetViewportOffset();
      this.viewportSyncFramesRemaining -= 1;
      if (this.viewportSyncFramesRemaining > 0) {
        this.viewportSyncRaf = requestAnimationFrame(step);
      }
    };
    this.viewportSyncRaf = requestAnimationFrame(step);
  }

  private focusSheetContainer(): void {
    requestAnimationFrame(() => {
      try {
        this.popover.focus({ preventScroll: true });
      } catch {
        this.popover.focus();
      }
    });
  }

  private ensureAnchorVisible(): void {
    if (!this.anchor) return;
    try {
      const coords = this.view.coordsAtPos(this.anchor.from);
      const safeTop = getTopViewportInset(16);
      const safeBottom = window.innerHeight - (this.renderMode === 'mobile-sheet' ? 260 : 24);
      if (coords.top < safeTop || coords.bottom > safeBottom) {
        const target = Math.max(0, window.scrollY + coords.top - Math.round(window.innerHeight * 0.3));
        window.scrollTo({ top: target, behavior: 'smooth' });
      }
    } catch {
      // best effort only
    }
  }

  private renderComposer(): void {
    const range = this.composeRange;
    const by = this.composeBy ?? getCurrentActor();
    if (!range) return;
    if (!canCommentInRuntime()) return;

    this.popover.innerHTML = '';

    const header = document.createElement('div');
    header.className = 'mark-popover-header';
    header.textContent = 'Add comment';

    const textarea = document.createElement('textarea');
    textarea.className = 'mark-popover-textarea';
    textarea.placeholder = 'Write a comment...';
    textarea.addEventListener('focus', () => {
      if (this.renderMode !== 'mobile-sheet') return;
      this.updateSheetViewportOffset();
      requestAnimationFrame(() => {
        try {
          textarea.scrollIntoView({ block: 'nearest' });
        } catch {
          // ignore browser quirks
        }
      });
    });

    const submit = () => {
      const text = textarea.value.trim();
      if (!text) return;
      const quote = this.view.state.doc.textBetween(range.from, range.to, '\n', '\n');
      const mark = addComment(this.view, quote, by, text, range);
      this.openForMark(mark.id);
    };

    textarea.addEventListener('keydown', event => {
      if ((event.key === 'Enter' && event.metaKey) || (this.renderMode === 'mobile-sheet' && event.key === 'Enter' && !event.shiftKey)) {
        event.preventDefault();
        submit();
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        this.close();
      }
    });

    const actions = document.createElement('div');
    actions.className = 'mark-popover-actions';

    const addButton = document.createElement('button');
    addButton.type = 'button';
    addButton.textContent = 'Add';
    installTouchSafeButton(addButton, () => {
      submit();
    });

    const cancelButton = document.createElement('button');
    cancelButton.type = 'button';
    cancelButton.textContent = 'Cancel';
    installTouchSafeButton(cancelButton, () => {
      this.close();
    });

    actions.appendChild(addButton);
    actions.appendChild(cancelButton);

    this.popover.appendChild(header);
    this.popover.appendChild(textarea);
    this.popover.appendChild(actions);
    requestAnimationFrame(() => {
      if (this.renderMode === 'mobile-sheet') {
        this.updateSheetViewportOffset();
        try {
          textarea.focus({ preventScroll: true });
        } catch {
          textarea.focus();
        }
        requestAnimationFrame(() => {
          this.updateSheetViewportOffset();
        });
        return;
      }
      textarea.focus();
    });
  }

  private renderThread(mark: Mark): void {
    const marks = getMarks(this.view.state);
    const data = mark.data as CommentData | undefined;
    const threadId = data?.thread;
    if (!threadId) return;

    const thread = getThread(marks, threadId);
    const canComment = canCommentInRuntime();
    this.lastThreadLength = thread.length;
    this.popover.innerHTML = '';

    const header = document.createElement('div');
    header.className = 'mark-popover-header';
    header.textContent = 'Comment thread';

    const list = document.createElement('div');
    list.className = 'mark-popover-thread';

    thread.forEach(entry => {
      const entryData = entry.data as CommentData | undefined;
      const entryEl = document.createElement('div');
      entryEl.className = 'mark-popover-entry';

      const meta = document.createElement('div');
      meta.className = 'mark-popover-meta';
      meta.textContent = `${getActorName(entry.by)} • ${formatTimestamp(entry.at)}`;

      const body = document.createElement('div');
      body.className = 'mark-popover-body';
      body.textContent = entryData?.text ?? '';

      entryEl.appendChild(meta);
      entryEl.appendChild(body);
      list.appendChild(entryEl);
    });

    const actions = document.createElement('div');
    actions.className = 'mark-popover-actions';
    let replyBox: HTMLTextAreaElement | null = null;

    if (canComment) {
      replyBox = document.createElement('textarea');
      replyBox.className = 'mark-popover-textarea';
      replyBox.placeholder = 'Reply...';
      replyBox.addEventListener('focus', () => {
        this.threadFocusMode = 'reply-box';
        if (this.renderMode !== 'mobile-sheet') return;
        this.updateSheetViewportOffset();
        requestAnimationFrame(() => {
          try {
            replyBox.scrollIntoView({ block: 'nearest' });
          } catch {
            // ignore browser quirks
          }
        });
      });

      const reply = () => {
        const text = replyBox.value.trim();
        if (!text) return;
        const proof = getProofEditorApi();
        const created = proof?.markReply
          ? proof.markReply(mark.id, getCurrentActor(), text)
          : replyToComment(this.view, mark.id, getCurrentActor(), text);
        if (!created) return;
        if (this.renderMode === 'mobile-sheet') {
          try {
            replyBox.blur();
          } catch {
            // ignore browser focus quirks
          }
          this.openForMark(mark.id, undefined, { threadFocusMode: 'sheet' });
          return;
        }
        this.openForMark(mark.id);
      };

      replyBox.addEventListener('keydown', event => {
        if ((event.key === 'Enter' && event.metaKey) || (this.renderMode === 'mobile-sheet' && event.key === 'Enter' && !event.shiftKey)) {
          event.preventDefault();
          reply();
        }
      });

      const replyButton = document.createElement('button');
      replyButton.type = 'button';
      replyButton.textContent = 'Reply';
      installTouchSafeButton(replyButton, () => {
        reply();
      });

      const resolved = thread.every(entry => {
        const entryData = entry.data as CommentData | undefined;
        return Boolean(entryData?.resolved);
      });

      const resolveButton = document.createElement('button');
      resolveButton.type = 'button';
      resolveButton.textContent = resolved ? 'Reopen' : 'Resolve';
      installTouchSafeButton(resolveButton, () => {
        const proof = getProofEditorApi();
        if (resolved) {
          if (proof?.markUnresolve) {
            proof.markUnresolve(mark.id);
          } else {
            unresolveComment(this.view, mark.id);
          }
          this.openForMark(mark.id);
        } else {
          if (proof?.markResolve) {
            proof.markResolve(mark.id);
          } else {
            resolveComment(this.view, mark.id);
          }
          this.close();
        }
      });

      const deleteButton = document.createElement('button');
      deleteButton.type = 'button';
      deleteButton.textContent = 'Delete';
      installTouchSafeButton(deleteButton, () => {
        const proof = getProofEditorApi();
        if (proof?.markDeleteThread) {
          proof.markDeleteThread(mark.id);
        } else {
          deleteMark(this.view, mark.id);
        }
        this.close();
      });

      actions.appendChild(replyButton);
      actions.appendChild(resolveButton);
      actions.appendChild(deleteButton);
    }

    const closeButton = document.createElement('button');
    closeButton.type = 'button';
    closeButton.textContent = canComment ? 'Close' : 'Done';
    installTouchSafeButton(closeButton, () => {
      this.close();
    });
    actions.appendChild(closeButton);

    this.popover.appendChild(header);
    this.popover.appendChild(list);
    if (replyBox) this.popover.appendChild(replyBox);
    this.popover.appendChild(actions);
    requestAnimationFrame(() => {
      if (!replyBox) {
        this.focusSheetContainer();
        return;
      }
      if (this.threadFocusMode === 'none') {
        return;
      }
      if (this.renderMode === 'mobile-sheet') {
        this.updateSheetViewportOffset();
        if (this.threadFocusMode === 'reply-box') {
          try {
            replyBox.focus({ preventScroll: true });
          } catch {
            replyBox.focus();
          }
        } else {
          this.popover.classList.remove('mark-popover-keyboard-open');
          this.focusSheetContainer();
        }
        requestAnimationFrame(() => {
          this.updateSheetViewportOffset();
        });
        return;
      }
      replyBox.focus();
    });
  }

  private renderSuggestion(mark: Mark): void {
    this.popover.innerHTML = '';
    const kind = isSuggestionKind(mark.kind) ? mark.kind : 'replace';
    const kindPresentation = getSuggestionKindPresentation(kind);

    const header = document.createElement('div');
    header.className = 'mark-popover-header';
    header.style.display = 'flex';
    header.style.alignItems = 'center';
    header.style.justifyContent = 'space-between';
    header.style.gap = '12px';

    const title = document.createElement('div');
    title.textContent = 'Suggestion';

    const kindBadge = document.createElement('div');
    kindBadge.style.cssText = [
      'display:inline-flex',
      'align-items:center',
      'gap:8px',
      'padding:4px 10px',
      'border-radius:999px',
      `border:1px solid ${kindPresentation.color}`,
      `background:${kindPresentation.tint}`,
      `color:${kindPresentation.color}`,
      'font-size:11px',
      'font-weight:700',
      'letter-spacing:0.02em',
      'text-transform:uppercase',
      'flex-shrink:0',
    ].join(';');

    const kindDot = document.createElement('span');
    kindDot.style.cssText = [
      'display:inline-block',
      'width:8px',
      'height:8px',
      'border-radius:999px',
      `background:${kindPresentation.color}`,
      'flex-shrink:0',
    ].join(';');
    kindBadge.append(kindDot, document.createTextNode(kindPresentation.label));
    header.append(title, kindBadge);

    const body = document.createElement('div');
    body.className = 'mark-popover-body';
    body.style.whiteSpace = 'normal';

    const appendDetailRow = (labelText: string, valueText: string, valueColor?: string): void => {
      if (!valueText) return;
      const label = document.createElement('div');
      label.textContent = labelText;
      label.style.cssText = 'font-size:11px;font-weight:600;letter-spacing:0.02em;text-transform:uppercase;opacity:0.62;margin-top:8px;';
      const value = document.createElement('div');
      value.textContent = valueText;
      value.style.cssText = [
        'white-space:pre-wrap',
        'word-break:break-word',
        'line-height:1.4',
        valueColor ? `color:${valueColor}` : '',
      ].filter(Boolean).join(';');
      body.appendChild(label);
      body.appendChild(value);
    };

    if (mark.kind === 'insert') {
      const data = mark.data as InsertData | undefined;
      appendDetailRow('Inserted text', data?.content ?? '', '#15803d');
    } else if (mark.kind === 'replace') {
      const data = mark.data as ReplaceData | undefined;
      const current = data?.content ?? '';
      const original = typeof data?.originalQuote === 'string' && data.originalQuote.trim().length > 0
        ? data.originalQuote
        : (mark.quote ?? '');
      appendDetailRow('Original text', original, '#b91c1c');
      appendDetailRow('Edited text', current, '#15803d');
    } else if (mark.kind === 'delete') {
      appendDetailRow('Deleted text', mark.quote ?? '', '#b91c1c');
    }

    const actions = document.createElement('div');
    actions.className = 'mark-popover-actions';
    const canEdit = canEditInRuntime();
    const previousMarkId = this.getAdjacentSuggestionMarkId(mark.id, 'prev');
    const nextMarkId = this.getAdjacentSuggestionMarkId(mark.id, 'next');

    const applyButton = document.createElement('button');
    applyButton.type = 'button';
    applyButton.textContent = 'Accept';
    applyButton.title = 'Accept change (Cmd/Ctrl+Alt+A)';
    installTouchSafeButton(applyButton, () => {
      if (!canEdit) return;
      this.runSuggestionReviewAction(mark.id, 'accept', nextMarkId);
    });

    const rejectButton = document.createElement('button');
    rejectButton.type = 'button';
    rejectButton.textContent = 'Reject';
    rejectButton.title = 'Reject change (Cmd/Ctrl+Alt+R)';
    installTouchSafeButton(rejectButton, () => {
      if (!canEdit) return;
      this.runSuggestionReviewAction(mark.id, 'reject', nextMarkId);
    });

    if (canEdit) {
      actions.appendChild(applyButton);
      actions.appendChild(rejectButton);
    }

    const previousButton = document.createElement('button');
    previousButton.type = 'button';
    previousButton.textContent = 'Previous';
    previousButton.title = 'Previous change (Cmd/Ctrl+Alt+[';
    installTouchSafeButton(previousButton, () => {
      this.navigateToSuggestion(previousMarkId);
    });
    previousButton.disabled = !previousMarkId;
    actions.appendChild(previousButton);

    const nextButton = document.createElement('button');
    nextButton.type = 'button';
    nextButton.textContent = 'Next';
    nextButton.title = 'Next change (Cmd/Ctrl+Alt+])';
    installTouchSafeButton(nextButton, () => {
      this.navigateToSuggestion(nextMarkId);
    });
    nextButton.disabled = !nextMarkId;
    actions.appendChild(nextButton);

    const closeButton = document.createElement('button');
    closeButton.type = 'button';
    closeButton.textContent = 'Close';
    installTouchSafeButton(closeButton, () => {
      this.close();
    });
    actions.appendChild(closeButton);

    this.popover.appendChild(header);
    this.popover.appendChild(body);
    this.popover.appendChild(actions);
  }

  private cacheActionRange(): void {
    const { from, to } = this.view.state.selection;
    if (from === to) return;
    const text = this.view.state.doc.textBetween(from, to);
    this.cachedActionRange = { range: { from, to }, text };
    this.cachedActionRangeAt = Date.now();
  }

  private getActiveSelectionClientRect(): DOMRect | null {
    const selection = document.getSelection();
    if (!selection || selection.rangeCount === 0) return null;
    if (!this.isSelectionWithinEditor(selection)) return null;
    const text = selection.toString().trim();
    if (!text) return null;
    try {
      const range = selection.getRangeAt(0);
      if (!range || typeof range.getBoundingClientRect !== 'function') return null;
      const rect = range.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) return null;
      return rect;
    } catch {
      return null;
    }
  }

  private getActionRange(): MarkRange | null {
    const { from, to } = this.view.state.selection;
    if (from !== to) {
      this.cacheActionRange();
      return { from, to };
    }
    if (this.cachedActionRange && (Date.now() - this.cachedActionRangeAt < MarkPopoverController.ACTION_RANGE_CACHE_TTL_MS)) {
      const { range, text } = this.cachedActionRange;
      try {
        const currentText = this.view.state.doc.textBetween(range.from, range.to);
        if (currentText === text) return range;
      } catch {
        // positions out of range after doc mutation
      }
    }
    if (isMobileTouch()) {
      const selection = document.getSelection();
      const selectedText = selection?.toString().trim() ?? '';
      if (selectedText && this.isSelectionWithinEditor(selection)) {
        const resolved = resolveQuoteRange(this.view.state.doc, selectedText);
        if (resolved) {
          const text = this.view.state.doc.textBetween(resolved.from, resolved.to);
          this.cachedActionRange = { range: resolved, text };
          this.cachedActionRangeAt = Date.now();
          return resolved;
        }
      }
    }
    return null;
  }

  private renderActionRow(): HTMLDivElement | null {
    if (!this.hasLiveSelection || !isMobileTouch()) return null;
    if (!canCommentInRuntime()) return null;

    const row = document.createElement('div');
    row.className = 'mark-mobile-strip-actions';

    const actions: Array<{ label: string; ariaLabel: string; handler: () => void }> = [
      {
        label: 'Comment',
        ariaLabel: 'Add comment on selected text',
        handler: () => {
          const range = this.getActionRange();
          if (!range) return;
          this.openComposer(range, getCurrentActor());
        },
      },
      {
        label: 'Flag',
        ariaLabel: 'Flag selected text',
        handler: () => {
          const range = this.getActionRange();
          if (!range) return;
          const actor = getCurrentActor();
          const quote = this.view.state.doc.textBetween(range.from, range.to, '\n', '\n');
          flag(this.view, quote, actor, undefined, range);
        },
      },
      {
        label: 'Suggest',
        ariaLabel: 'Suggest replacement for selected text',
        handler: () => {
          const range = this.getActionRange();
          if (!range) return;
          const actor = getCurrentActor();
          const replacement = window.prompt('Replace with:');
          if (replacement === null) return;
          const quote = this.view.state.doc.textBetween(range.from, range.to, '\n', '\n');
          suggestReplace(this.view, quote, actor, replacement, range);
        },
      },
    ];

    for (const { label, ariaLabel, handler } of actions) {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.textContent = label;
      btn.setAttribute('aria-label', ariaLabel);
      installTouchSafeButton(btn, () => {
        handler();
      }, {
        onPointerDown: () => {
          this.cacheActionRange();
        },
      });
      row.appendChild(btn);
    }

    return row;
  }

  private renderMobileStrip(): void {
    if (!shouldUseCommentUiV2()) {
      this.strip.style.display = 'none';
      this.mobileStripSignature = '';
      this.mobileStripExpanded = false;
      this.strip.classList.remove('mark-mobile-strip-expanded');
      this.clearMobileStripPadding();
      return;
    }
    if (this.mode !== null) {
      this.strip.style.display = 'none';
      this.mobileStripExpanded = false;
      this.mobileStripSignature = '';
      this.strip.classList.remove('mark-mobile-strip-expanded');
      this.clearMobileStripPadding();
      return;
    }

    // Sync hasLiveSelection from DOM on every render cycle.
    // On iOS, getDomSelectionRange() can return null during RAF even when a
    // selection is active (the native selection UI holds it but posAtDOM fails
    // in certain timing windows). Trust the cached range from handleSelectionChange
    // if it's fresh (< 12s) — only upgrade hasLiveSelection, never downgrade it
    // when a cached range exists.
    if (isMobileTouch()) {
      const selection = document.getSelection();
      const selectionCleared = !selection || selection.rangeCount === 0;
      const hasDomSelectionInsideEditor = this.isSelectionWithinEditor(selection);
      const hasDomSelectionText = Boolean(
        selection && selection.toString().trim().length > 0 && hasDomSelectionInsideEditor
      );
      const domRange = this.getDomSelectionRange();
      const pmRange = this.getProseMirrorSelectionRange();
      const effectiveRange = domRange ?? pmRange;
      const hasSelection = Boolean(effectiveRange && effectiveRange.from !== effectiveRange.to);
      const hasFreshCache = this.hasFreshCachedActionRange();
      if (hasSelection && !this.hasLiveSelection) {
        // DOM says there's a selection but handler missed it — upgrade
        this.hasLiveSelection = true;
        const { from, to } = effectiveRange!;
        const text = this.view.state.doc.textBetween(from, to);
        this.cachedActionRange = { range: { from, to }, text };
        this.cachedActionRangeAt = Date.now();
        if (this.mobileStripExpanded) {
          this.mobileStripExpanded = false;
        }
        this.mobileStripSignature = ''; // force re-render
      } else if (!hasSelection && hasDomSelectionText && !this.hasLiveSelection) {
        // iOS can expose a real text selection while PM/DOM position mapping
        // is temporarily unavailable (selection handles/callout timing).
        this.hasLiveSelection = true;
        this.mobileStripSignature = '';
      } else if (selectionCleared && this.hasLiveSelection) {
        this.hasLiveSelection = false;
        this.mobileStripSignature = '';
      } else if (!hasSelection && !hasDomSelectionText && this.hasLiveSelection && !hasFreshCache) {
        // DOM says no selection AND cache is stale — safe to downgrade
        this.hasLiveSelection = false;
        this.mobileStripSignature = '';
      } else if (hasSelection && effectiveRange) {
        // Update cached range if DOM range changed
        const cached = this.cachedActionRange?.range;
        if (!cached || cached.from !== effectiveRange.from || cached.to !== effectiveRange.to) {
          const { from, to } = effectiveRange;
          const text = this.view.state.doc.textBetween(from, to);
          this.cachedActionRange = { range: { from, to }, text };
          this.cachedActionRangeAt = Date.now();
          this.mobileStripSignature = '';
        }
      }
    }

    const comments = this.getMobileCommentData();
    const hasActionRow = this.hasLiveSelection && isMobileTouch();
    const canShowActionRow = hasActionRow && canCommentInRuntime();
    if (comments.totalCount === 0 && !canShowActionRow) {
      this.strip.style.display = 'none';
      this.mobileStripSignature = '';
      this.mobileStripExpanded = false;
      this.strip.classList.remove('mark-mobile-strip-expanded');
      this.clearMobileStripPadding();
      return;
    }

    const signature = this.buildMobileStripSignature(comments);
    if (signature === this.mobileStripSignature) {
      this.strip.classList.toggle('mark-mobile-strip-selection', canShowActionRow && !this.mobileStripExpanded);
      if (this.mobileStripExpanded) {
        this.strip.style.display = 'block';
      } else {
        this.strip.style.display = canShowActionRow ? 'block' : 'flex';
      }
      this.strip.classList.toggle('mark-mobile-strip-expanded', this.mobileStripExpanded);
      this.updateSheetViewportOffset();
      // Keep the floating action row anchored after viewport/scroll updates.
      this.positionMobileSelectionActions(canShowActionRow);
      this.scheduleMobileStripPadding();
      return;
    }

    this.strip.innerHTML = '';
    this.strip.classList.toggle('mark-mobile-strip-expanded', this.mobileStripExpanded);
    this.strip.classList.toggle('mark-mobile-strip-selection', canShowActionRow && !this.mobileStripExpanded);
    if (!this.mobileStripExpanded) {
      const actionRow = this.renderActionRow();
      if (actionRow) {
        this.strip.appendChild(actionRow);
      }
      if (comments.totalCount > 0 && !canShowActionRow) {
        this.renderMobileStripSummary(comments);
      }
    } else {
      this.renderMobileStripExpanded(comments);
    }

    this.mobileStripSignature = signature;
    if (this.mobileStripExpanded) {
      this.strip.style.display = 'block';
    } else {
      this.strip.style.display = canShowActionRow ? 'block' : 'flex';
    }
    this.updateSheetViewportOffset();
    this.positionMobileSelectionActions(canShowActionRow);
    this.scheduleMobileStripPadding();
  }

  private positionMobileSelectionActions(canShowActionRow: boolean): void {
    if (!canShowActionRow || this.mobileStripExpanded || this.strip.style.display === 'none') {
      this.strip.style.top = '';
      this.strip.style.left = '';
      this.strip.style.right = '';
      return;
    }
    const selectionRect = this.getActiveSelectionClientRect();
    if (!selectionRect) {
      this.strip.style.top = '';
      this.strip.style.left = '';
      this.strip.style.right = '';
      return;
    }
    try {
      if (typeof this.strip.getBoundingClientRect !== 'function') return;
      const vv = window.visualViewport;
      const viewportHeight = getVisualViewportHeight(window.innerHeight, vv ?? null);
      const safeTop = getTopViewportInset(12);
      const stripRect = this.strip.getBoundingClientRect();
      const maxTop = Math.max(safeTop, viewportHeight - stripRect.height - 12);
      const belowTop = selectionRect.bottom + 10;
      const aboveTop = selectionRect.top - stripRect.height - 10;
      const targetTop = belowTop <= maxTop ? belowTop : clamp(aboveTop, safeTop, maxTop);
      const targetLeft = clamp(
        selectionRect.left + (selectionRect.width / 2) - (stripRect.width / 2),
        12,
        window.innerWidth - stripRect.width - 12
      );
      this.strip.style.top = `${targetTop}px`;
      this.strip.style.left = `${targetLeft}px`;
      this.strip.style.right = 'auto';
      this.strip.style.bottom = 'auto';
    } catch {
      // Ignore invalid rect reads (prevents crash on remote comment inserts).
    }
  }

  private scheduleMobileStripPadding(): void {
    if (this.mobileStripPaddingRaf !== null) {
      cancelAnimationFrame(this.mobileStripPaddingRaf);
    }
    this.mobileStripPaddingRaf = requestAnimationFrame(() => {
      this.mobileStripPaddingRaf = null;
      if (!shouldUseCommentUiV2() || this.mode !== null || this.strip.style.display === 'none') {
        this.clearMobileStripPadding();
        return;
      }
      if (this.strip.classList.contains('mark-mobile-strip-selection')) {
        this.clearMobileStripPadding();
        return;
      }
      try {
        if (typeof this.strip.getBoundingClientRect !== 'function') {
          this.clearMobileStripPadding();
          return;
        }
        const rect = this.strip.getBoundingClientRect();
        if (rect.height <= 0) {
          this.clearMobileStripPadding();
          return;
        }
        this.applyMobileStripPadding(Math.ceil(rect.height + MOBILE_STRIP_PADDING_EXTRA));
      } catch {
        this.clearMobileStripPadding();
      }
    });
  }

  private getMobileStripPaddingTarget(): HTMLElement | null {
    return (this.view.dom.closest('.ProseMirror') as HTMLElement | null) ?? this.view.dom;
  }

  private applyMobileStripPadding(padding: number): void {
    if (!this.view.dom.isConnected || !this.strip.isConnected) {
      this.clearMobileStripPadding();
      return;
    }
    const target = this.getMobileStripPaddingTarget();
    if (!target) return;
    if (this.mobileStripPaddingTarget && this.mobileStripPaddingTarget !== target) {
      this.clearMobileStripPadding();
    }
    if (!this.mobileStripPaddingTarget) {
      this.mobileStripPaddingTarget = target;
      this.mobileStripPaddingOriginal = target.style.paddingBottom || '';
      const computed = window.getComputedStyle(target).paddingBottom;
      const parsed = Number.parseFloat(computed);
      this.mobileStripPaddingBase = Number.isFinite(parsed) ? parsed : 0;
    }
    const basePadding = this.mobileStripPaddingBase ?? 0;
    target.style.paddingBottom = `${basePadding + padding}px`;
  }

  private clearMobileStripPadding(): void {
    if (!this.mobileStripPaddingTarget) return;
    this.mobileStripPaddingTarget.style.paddingBottom = this.mobileStripPaddingOriginal ?? '';
    this.mobileStripPaddingTarget = null;
    this.mobileStripPaddingOriginal = null;
    this.mobileStripPaddingBase = null;
  }

  private scheduleMobileStripRender(immediate = false): void {
    if (immediate) {
      // On iOS, selection state can be overwritten between RAF scheduling and
      // execution (e.g. by ProseMirror update cycles). When the caller knows
      // the selection state is authoritative (handleSelectionChange), render
      // synchronously to avoid the race.
      this.mobileStripRafScheduled = false;
      this.renderMobileStrip();
      return;
    }
    if (this.mobileStripRafScheduled) return;
    this.mobileStripRafScheduled = true;
    requestAnimationFrame(() => {
      this.mobileStripRafScheduled = false;
      this.renderMobileStrip();
    });
  }

  private buildMobileStripSignature(data: MobileCommentData): string {
    const nearbyIds = data.nearby.map(({ mark }) => mark.id).join(',');
    const allIds = data.all.map(({ mark }) => mark.id).join(',');
    const sel = this.view.state.selection;
    const cachedRange = this.cachedActionRange?.range;
    const hasActionRow = this.hasLiveSelection && isMobileTouch() && canCommentInRuntime();
    const selPart = hasActionRow
      ? (isMobileTouch() && cachedRange
        ? `sel:1:${cachedRange.from}-${cachedRange.to}`
        : `sel:1:${sel.from}-${sel.to}`)
      : 'sel:0';
    return `${selPart}|${this.mobileStripExpanded ? 'expanded' : 'collapsed'}:${data.nearbyCount}:${data.totalCount}:${nearbyIds}:${allIds}`;
  }

  private createMobileCommentCard(mark: Mark): HTMLButtonElement {
    const data = mark.data as CommentData | undefined;
    const card = document.createElement('button');
    card.type = 'button';
    card.className = 'mark-mobile-card';
    card.setAttribute('data-mark-id', mark.id);
    const actor = document.createElement('strong');
    actor.textContent = getActorName(mark.by);
    const preview = document.createElement('span');
    preview.textContent = (data?.text ?? '').slice(0, 90) || 'Comment';
    card.append(actor, preview);
    installTouchSafeButton(card, () => {
      this.openForMark(mark.id);
    }, {
      preventTouchPointerDown: false,
      stopPointerDownPropagation: false,
    });
    return card;
  }

  private renderMobileStripSummary(data: MobileCommentData): void {
    const summary = document.createElement('button');
    summary.type = 'button';
    summary.className = 'mark-mobile-summary';

    const title = document.createElement('span');
    title.className = 'mark-mobile-summary-title';
    title.textContent = `Comments (${data.totalCount})`;

    const meta = document.createElement('span');
    meta.className = 'mark-mobile-summary-meta';
    meta.textContent = data.nearbyCount > 0
      ? `${data.nearbyCount} nearby · ${data.totalCount} total`
      : `No nearby comments · ${data.totalCount} total`;

    const cta = document.createElement('span');
    cta.className = 'mark-mobile-summary-cta';
    cta.textContent = 'Open';

    summary.append(title, meta, cta);
    installTouchSafeButton(summary, () => {
      this.mobileStripExpanded = true;
      this.mobileStripSignature = '';
      this.scheduleMobileStripRender();
    });

    this.strip.appendChild(summary);
  }

  private renderMobileStripSection(label: string, comments: VisibleComment[]): void {
    if (comments.length === 0) return;
    const section = document.createElement('div');
    section.className = 'mark-mobile-section';

    const heading = document.createElement('div');
    heading.className = 'mark-mobile-section-heading';
    heading.textContent = label;

    section.appendChild(heading);
    comments.forEach(({ mark }) => {
      section.appendChild(this.createMobileCommentCard(mark));
    });
    this.strip.appendChild(section);
  }

  private renderMobileStripExpanded(data: MobileCommentData): void {
    const header = document.createElement('div');
    header.className = 'mark-mobile-strip-header';

    const titleBlock = document.createElement('div');
    titleBlock.className = 'mark-mobile-strip-heading';

    const title = document.createElement('strong');
    title.textContent = `Comments (${data.totalCount})`;

    const subtitle = document.createElement('span');
    subtitle.textContent = data.nearbyCount > 0
      ? `${data.nearbyCount} nearby in view`
      : 'No nearby comments in view';

    titleBlock.append(title, subtitle);

    const done = document.createElement('button');
    done.type = 'button';
    done.className = 'mark-mobile-strip-done';
    done.textContent = 'Done';
    installTouchSafeButton(done, () => {
      this.mobileStripExpanded = false;
      this.mobileStripSignature = '';
      this.scheduleMobileStripRender();
    });

    header.append(titleBlock, done);
    this.strip.appendChild(header);

    const nearby = data.nearby.slice(0, 6);
    this.renderMobileStripSection('Nearby', nearby);

    const nearbyIds = new Set(nearby.map(({ mark }) => mark.id));
    const remaining = data.all.filter(({ mark }) => !nearbyIds.has(mark.id)).slice(0, 12);
    if (remaining.length > 0) {
      this.renderMobileStripSection('All comments', remaining);
    } else if (nearby.length === 0) {
      this.renderMobileStripSection('All comments', data.all.slice(0, 12));
    } else {
      const note = document.createElement('div');
      note.className = 'mark-mobile-strip-note';
      note.textContent = 'All open comments are nearby.';
      this.strip.appendChild(note);
    }
  }

  private getMobileCommentData(): MobileCommentData {
    const marks = getMarks(this.view.state).filter(isResolvableComment);
    const all = marks
      .map(mark => {
        const range = resolveAnchorRange(this.view, mark);
        return range ? { mark, range } : null;
      })
      .filter((entry): entry is VisibleComment => Boolean(entry));

    const visibleTop = Math.max(0, getTopViewportInset(0) - 100);
    const visibleBottom = window.innerHeight + 220;
    const result: VisibleComment[] = [];

    for (const entry of all) {
      try {
        const coords = this.view.coordsAtPos(entry.range.from);
        if (coords.bottom < visibleTop || coords.top > visibleBottom) continue;
        result.push(entry);
      } catch {
        // ignore stale coordinates
      }
    }

    return {
      nearby: result.slice(0, 8),
      all: all.slice(0, 18),
      nearbyCount: result.length,
      totalCount: marks.length,
    };
  }

  private resolveFromStrip(markId: string): void {
    if (!canCommentInRuntime()) return;
    const resolved = resolveComment(this.view, markId);
    if (resolved) {
      this.showUndoToast(markId);
    }
    this.scheduleMobileStripRender();
    if (this.activeMarkId === markId) {
      this.close();
    }
  }

  private showUndoToast(markId: string): void {
    this.undoMarkId = markId;
    this.undoToast.innerHTML = '';

    const label = document.createElement('span');
    label.textContent = 'Comment resolved';

    const undoButton = document.createElement('button');
    undoButton.type = 'button';
    undoButton.textContent = 'Undo';
    installTouchSafeButton(undoButton, () => {
      if (!this.undoMarkId) return;
      unresolveComment(this.view, this.undoMarkId);
      this.scheduleMobileStripRender();
      this.clearUndoToast();
    });

    this.undoToast.appendChild(label);
    this.undoToast.appendChild(undoButton);
    this.undoToast.style.display = 'flex';
    this.updateSheetViewportOffset();

    if (this.undoTimer !== null) {
      window.clearTimeout(this.undoTimer);
    }
    this.undoTimer = window.setTimeout(() => {
      this.clearUndoToast();
    }, 5000);
  }

  private clearUndoToast(): void {
    this.undoMarkId = null;
    this.undoToast.style.display = 'none';
    this.undoToast.innerHTML = '';
    if (this.undoTimer !== null) {
      window.clearTimeout(this.undoTimer);
      this.undoTimer = null;
    }
  }

  private resetStripGestureVisual(): void {
    this.stripGestureStartX = null;
    if (!this.stripGestureCard) return;
    this.stripGestureCard.style.transition = 'transform 120ms ease, opacity 120ms ease';
    this.stripGestureCard.style.transform = '';
    this.stripGestureCard.style.opacity = '';
    this.stripGestureCard = null;
  }

  private getDraftTextarea(): HTMLTextAreaElement | null {
    const textarea = this.popover.querySelector('.mark-popover-textarea');
    return textarea instanceof HTMLTextAreaElement ? textarea : null;
  }

  captureDraftSnapshot(): CommentPopoverDraftSnapshot | null {
    if (this.mode !== 'composer' && this.mode !== 'thread') return null;
    const textarea = this.getDraftTextarea();
    if (!textarea) return null;
    const draftText = textarea.value;
    if (!draftText.trim()) return null;

    if (this.mode === 'composer') {
      const range = this.composeRange;
      if (!range) return null;
      return {
        mode: 'composer',
        range: { from: range.from, to: range.to },
        by: this.composeBy ?? getCurrentActor(),
        text: draftText,
      };
    }

    if (!this.activeMarkId) return null;
    return {
      mode: 'thread',
      markId: this.activeMarkId,
      by: getCurrentActor(),
      text: draftText,
    };
  }

  restoreDraftSnapshot(snapshot: CommentPopoverDraftSnapshot): boolean {
    if (snapshot.mode === 'composer') {
      const docSize = Math.max(1, this.view.state.doc.content.size);
      const from = clamp(snapshot.range.from, 1, docSize);
      const to = clamp(snapshot.range.to, from, docSize);
      if (from === to) return false;
      this.openComposer({ from, to }, snapshot.by);
    } else {
      const mark = getMarks(this.view.state).find((entry) => entry.id === snapshot.markId);
      if (!mark || mark.kind !== 'comment') return false;
      this.openForMark(snapshot.markId);
    }

    const textarea = this.getDraftTextarea();
    if (!textarea) return false;
    textarea.value = snapshot.text;
    requestAnimationFrame(() => {
      const end = textarea.value.length;
      textarea.setSelectionRange(end, end);
    });
    return true;
  }
}

export function openCommentComposer(view: EditorView, range: MarkRange, by: string): void {
  if (!canCommentInRuntime()) return;
  const controller = controllers.get(view);
  if (!controller) return;
  controller.openComposer(range, by);
}

export function captureCommentPopoverDraft(view: EditorView): CommentPopoverDraftSnapshot | null {
  const controller = controllers.get(view);
  if (!controller) return null;
  return controller.captureDraftSnapshot();
}

export function restoreCommentPopoverDraft(
  view: EditorView,
  snapshot: CommentPopoverDraftSnapshot
): boolean {
  const controller = controllers.get(view);
  if (!controller) return false;
  return controller.restoreDraftSnapshot(snapshot);
}

export const markPopoverPlugin = $prose(() => {
  return new Plugin({
    key: markPopoverKey,
    view(view) {
      const controller = new MarkPopoverController(view);
      controllers.set(view, controller);
      return controller;
    }
  });
});
