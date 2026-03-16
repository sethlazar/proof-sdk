import { readFileSync } from 'node:fs';
import path from 'node:path';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

function sliceBetween(source: string, startNeedle: string, endNeedle: string): string {
  const start = source.indexOf(startNeedle);
  assert(start !== -1, `Missing block start: ${startNeedle}`);
  const end = source.indexOf(endNeedle, start);
  assert(end !== -1, `Missing block end after: ${startNeedle}`);
  return source.slice(start, end);
}

function run(): void {
  const editorSource = readFileSync(path.resolve(process.cwd(), 'src/editor/index.ts'), 'utf8');
  const editorHtmlSource = readFileSync(path.resolve(process.cwd(), 'src/index.html'), 'utf8');
  const keybindingConfigSource = readFileSync(path.resolve(process.cwd(), 'src/editor/keybindings-config.ts'), 'utf8');
  const keybindingsSource = readFileSync(path.resolve(process.cwd(), 'src/editor/plugins/keybindings.ts'), 'utf8');
  const marksSource = readFileSync(path.resolve(process.cwd(), 'src/editor/plugins/marks.ts'), 'utf8');
  const popoverSource = readFileSync(path.resolve(process.cwd(), 'src/editor/plugins/mark-popover.ts'), 'utf8');
  const contextMenuSource = readFileSync(path.resolve(process.cwd(), 'src/ui/context-menu.ts'), 'utf8');
  const suggestionsSource = readFileSync(path.resolve(process.cwd(), 'src/editor/plugins/suggestions.ts'), 'utf8');
  const collabCursorSource = readFileSync(path.resolve(process.cwd(), 'src/editor/plugins/collab-cursors.ts'), 'utf8');
  const shareClientSource = readFileSync(path.resolve(process.cwd(), 'src/bridge/share-client.ts'), 'utf8');
  const agentRoutesSource = readFileSync(path.resolve(process.cwd(), 'server/agent-routes.ts'), 'utf8');
  const markRehydrationSource = readFileSync(path.resolve(process.cwd(), 'server/proof-mark-rehydration.ts'), 'utf8');

  const acceptSuggestionBlock = sliceBetween(editorSource, '  acceptSuggestion(id: string): boolean {', '\n  /**');
  assert(acceptSuggestionBlock.includes('return this.markAccept(String(id));'), 'Expected acceptSuggestion to delegate to markAccept');

  const getTrackChangesViewModeBlock = sliceBetween(editorSource, '  getTrackChangesViewMode(): SuggestionDisplayMode {', '\n  /**');
  assert(
    getTrackChangesViewModeBlock.includes('mode = getSuggestionDisplayMode(view.state);'),
    'Expected getTrackChangesViewMode to read the marks plugin display mode',
  );

  const setTrackChangesViewModeBlock = sliceBetween(editorSource, '  setTrackChangesViewMode(mode: SuggestionDisplayMode): SuggestionDisplayMode {', '\n  /**');
  assert(
    setTrackChangesViewModeBlock.includes('persistTrackChangesViewMode(nextMode);')
      && setTrackChangesViewModeBlock.includes('this.trackChangesViewMode = setSuggestionDisplayMode(view, nextMode);')
      && setTrackChangesViewModeBlock.includes('applyTrackChangesViewModeToDom(view, this.trackChangesViewMode);'),
    'Expected setTrackChangesViewMode to persist, dispatch the marks plugin display mode, and tag the editor DOM with the active track-changes view',
  );
  assert(
    editorSource.includes('function applyTrackChangesViewModeToDom(')
      && editorSource.includes('view.dom.dataset.trackChangesView = normalizeTrackChangesViewMode(mode);')
      && editorSource.includes('applyTrackChangesViewModeToDom(view, this.trackChangesViewMode);')
      && editorSource.includes('applyTrackChangesViewModeToDom(view, mode);')
      && editorSource.includes('applyTrackChangesViewModeToDom(view, nextMode);'),
    'Expected track-changes mode changes to keep the editor DOM data attribute in sync for view-specific styling',
  );
  assert(
    editorHtmlSource.includes('.ProseMirror[data-track-changes-view="simple"] span[data-proof="suggestion"] {')
      && editorHtmlSource.includes('background: transparent !important;')
      && editorHtmlSource.includes('text-decoration: none !important;')
      && editorHtmlSource.includes('.ProseMirror[data-track-changes-view="simple"] .mark-replace-insert.mark-simple {')
      && editorHtmlSource.includes('border-bottom: none !important;'),
    'Expected simple markup to neutralize the legacy inline suggestion wrapper styling and remove replacement highlight blocks',
  );
  assert(
    marksSource.includes("change_indicator: 'display:inline-flex;width:7px;height:7px;margin:0 2px;")
      && collabCursorSource.includes('top: -6px;')
      && collabCursorSource.includes('transform: translateY(-100%);'),
    'Expected simple-markup deletion dots to be subtler and collaborator labels to sit above the text line',
  );
  assert(
    editorSource.includes("addModeItem('No markup'")
      && editorSource.includes("addModeItem('Original'"),
    'Expected the share menu to expose the extra Word-style track-changes modes',
  );
  assert(
    keybindingConfigSource.includes("acceptSuggestionAndNext: 'Mod-Alt-a'")
      && keybindingConfigSource.includes("rejectSuggestionAndNext: 'Mod-Alt-r'")
      && keybindingConfigSource.includes("undo: 'Mod-z'")
      && keybindingConfigSource.includes("redo: 'Mod-Shift-z'")
      && keybindingConfigSource.includes("nextSuggestion: 'Mod-Alt-]'")
      && keybindingConfigSource.includes("prevSuggestion: 'Mod-Alt-['")
      && keybindingConfigSource.includes("toggleTrackChanges: 'Mod-Shift-e'")
      && keybindingsSource.includes("import { proofKeybindingConfig } from '../keybindings-config';")
      && keybindingsSource.includes('[proofKeybindingConfig.undo]: undoCommand')
      && keybindingsSource.includes('[proofKeybindingConfig.redo]: redoCommand')
      && keybindingsSource.includes('if (!proof?.undo) return false;')
      && keybindingsSource.includes('if (!proof?.redo) return false;')
      && keybindingsSource.includes('[proofKeybindingConfig.acceptSuggestionAndNext]: acceptActiveSuggestionCommand')
      && keybindingsSource.includes('[proofKeybindingConfig.rejectSuggestionAndNext]: rejectActiveSuggestionCommand')
      && keybindingsSource.includes('[proofKeybindingConfig.nextSuggestion]: navigateNextSuggestionCommand')
      && keybindingsSource.includes('[proofKeybindingConfig.prevSuggestion]: navigatePrevSuggestionCommand')
      && keybindingsSource.includes('[proofKeybindingConfig.toggleTrackChanges]: toggleSuggestionsCommand')
      && keybindingsSource.includes('const nextSuggestionId = getSuggestionReviewFollowupId(state, activeId);')
      && keybindingsSource.includes("persistSuggestionReviewAndAdvance(activeId, nextSuggestionId, 'accept');")
      && keybindingsSource.includes("persistSuggestionReviewAndAdvance(activeId, nextSuggestionId, 'reject');"),
    'Expected dedicated keyboard shortcuts to live in a shared config file, including native undo/redo bindings and advance review after accepting or rejecting suggestions',
  );
  assert(
    editorSource.includes('private createTrackChangesModeToggle(): HTMLElement {')
      && editorSource.includes("makeSegment('Edit'")
      && editorSource.includes("makeSegment('Track Changes'"),
    'Expected the share banner to expose a visible Edit / Track Changes toggle',
  );
  assert(
    editorSource.includes('undo(): boolean {')
      && editorSource.includes('redo(): boolean {')
      && editorSource.includes('private undoSnapshotStack: string[] = [];')
      && editorSource.includes('private redoSnapshotStack: string[] = [];')
      && editorSource.includes('private currentUndoSnapshot: string | null = null;')
      && editorSource.includes('private undoTypingGroupActive: boolean = false;')
      && editorSource.includes('private getUndoSnapshotIntent(transaction: any, state: EditorState): UndoSnapshotIntent | null {')
      && editorSource.includes('private recordUndoSnapshot(')
      && editorSource.includes('const shouldCoalesceWithPrevious = Boolean(intent?.coalesce && this.undoTypingGroupActive);')
      && editorSource.includes('this.undoTypingGroupActive = Boolean(intent?.coalesce && intent.continueGroupAfter);')
      && editorSource.includes('const undoSnapshotIntent = this.getUndoSnapshotIntent(tr, view.state);')
      && editorSource.includes('private ensureUndoSnapshotInitialized(): void {')
      && editorSource.includes('private restoreUndoSnapshot(snapshot: string): boolean {')
      && editorSource.includes('preserveHistory: true,')
      && editorSource.includes('this.ensureUndoSnapshotInitialized();')
      && editorSource.includes('if (this.collabEnabled) {')
      && editorSource.includes('handled = collabUndo(view.state);')
      && editorSource.includes('handled = collabRedo(view.state);')
      && editorSource.includes('const commands = ctx.get(commandsCtx);')
      && editorSource.includes('handled = commands.call(undoCommand.key);')
      && editorSource.includes('handled = commands.call(redoCommand.key);')
      && editorSource.includes('private createHistoryControls(): HTMLElement {')
      && editorSource.includes("const undoButton = makeButton('↶', 'Undo (Cmd/Ctrl+Z)', () => this.undo());")
      && editorSource.includes("const redoButton = makeButton('↷', 'Redo (Shift+Cmd/Ctrl+Z)', () => this.redo());")
      && editorSource.includes('banner.replaceChildren(wordmark, separator, title, historyControls, trackChangesToggle')
      && editorSource.includes("container.className = 'share-pill-history-controls';")
      && editorSource.includes("button.className = 'share-pill-history-btn';"),
    'Expected the share banner to expose undo/redo controls backed by a user-visible snapshot stack in share mode, with word-level typing coalescing and command fallbacks elsewhere',
  );
  assert(
    suggestionsSource.includes("setMeta(suggestionsPluginKey, { enabled: true })")
      && suggestionsSource.includes("setMeta(suggestionsPluginKey, { enabled: false })")
      && suggestionsSource.includes(".setMeta('addToHistory', false);")
      && editorSource.includes(".setMeta('document-load', true)")
      && editorSource.includes(".setMeta('addToHistory', false);")
      && editorSource.includes("view.dispatch(markTr.setMeta('addToHistory', false));")
      && editorSource.includes("setMeta('heatmapUpdate', true)")
      && editorSource.includes("setMeta('addToHistory', false);")
      && editorSource.includes('if (!options?.preserveHistory) {')
      && editorSource.includes('this.resetUndoSnapshots(view, ctx.get(serializerCtx));')
      && editorSource.includes('this.recordUndoSnapshot(view, serializer, intent);'),
    'Expected document-load, suggestion-toggle, mark rehydration, and heatmap bookkeeping transactions to stay out of the undo history while typed content changes still record through the transaction-aware snapshot path',
  );
  assert(
    popoverSource.includes("view.dom.addEventListener('mousemove', this.handleEditorMouseMove);")
      && popoverSource.includes("'desktop-side-panel'")
      && popoverSource.includes('function getSuggestionKindPresentation(')
      && popoverSource.includes("label: 'Insertion'")
      && popoverSource.includes("label: 'Deletion'")
      && popoverSource.includes("label: 'Replacement'")
      && popoverSource.includes('private getPreferredRenderMode(mode: PopoverMode): RenderMode {')
      && popoverSource.includes('positionSidePanel(')
      && popoverSource.includes('function getVisibleRenderedMarkBox(')
      && popoverSource.includes("view.dom.querySelectorAll(`[data-mark-id=\"${escapedMarkId}\"]`)")
      && popoverSource.includes('const renderedBox = getVisibleRenderedMarkBox(view, markId);')
      && popoverSource.includes("source?: 'direct' | 'hover'")
      && popoverSource.includes("appendDetailRow('Original text', original")
      && popoverSource.includes("private renderSuggestionRail(): void {")
      && popoverSource.includes("getSuggestionDisplayMode(this.view.state) !== 'simple'")
      && popoverSource.includes("this.suggestionRail.className = 'mark-suggestion-rail';")
      && popoverSource.includes("button.className = 'mark-suggestion-rail-button';")
      && popoverSource.includes('const RAIL_JOIN_THRESHOLD_PX = 30;')
      && popoverSource.includes('const connectPrev = prevCenterY !== null && (centerY - prevCenterY) <= RAIL_JOIN_THRESHOLD_PX;')
      && popoverSource.includes('const connectNext = nextCenterY !== null && (nextCenterY - centerY) <= RAIL_JOIN_THRESHOLD_PX;')
      && popoverSource.includes('const segmentWidth = isConnectedSegment ? 12 : 18;')
      && popoverSource.includes("const borderRadius = connectPrev")
      && popoverSource.includes("button.textContent = '';")
      && popoverSource.includes("const badge = document.createElement('span');")
      && popoverSource.includes('const labelText = item.markIds.length > 1 ? String(item.markIds.length) : \'\';')
      && popoverSource.includes("const changeLabel = item.markIds.length === 1 ? 'change' : 'changes';")
      && popoverSource.includes('button.title = `${item.markIds.length} pending ${changeLabel} on this line`;')
      && popoverSource.includes("matchesReviewShortcut(event, { key: 'a', code: 'KeyA' })")
      && popoverSource.includes("matchesReviewShortcut(event, { key: 'r', code: 'KeyR' })")
      && popoverSource.includes("matchesReviewShortcut(event, { key: ']', code: 'BracketRight' })")
      && popoverSource.includes("matchesReviewShortcut(event, { key: '[', code: 'BracketLeft' })")
      && popoverSource.includes('const stateActiveMarkId = getActiveMarkId(view.state);')
      && popoverSource.includes("if (this.mode === 'suggestion') {")
      && popoverSource.includes('if (stateActiveMarkId && stateActiveMarkId !== this.activeMarkId) {')
      && popoverSource.includes("document.addEventListener('keydown', this.handleKeydown, true);")
      && popoverSource.includes("document.removeEventListener('keydown', this.handleKeydown, true);")
      && popoverSource.includes("private runSuggestionReviewAction(")
      && popoverSource.includes('REVIEW_ACTION_MAX_RETRIES')
      && popoverSource.includes("this.runSuggestionReviewAction(mark.id, 'accept', nextMarkId, mark.kind);")
      && popoverSource.includes("this.runSuggestionReviewAction(mark.id, 'reject', nextMarkId, mark.kind);")
      && popoverSource.includes('const persistedAction = action === \'accept\'')
      && popoverSource.includes("_suggestionKind: 'insert' | 'delete' | 'replace'")
      && popoverSource.includes('const allowOptimisticAccept = false;')
      && popoverSource.includes('const optimisticApplied = allowOptimisticAccept ? runLocalActionOnly() : false;')
      && popoverSource.includes('return acceptSuggestion(this.view, markId);')
      && popoverSource.includes('setReviewButtonsBusy(true);')
      && popoverSource.includes('private getSuggestionReviewFollowupMarkId(')
      && popoverSource.includes('private openSuggestionAfterReview(')
      && popoverSource.includes('this.openSuggestionAfterReview(nextMarkId, markId);')
      && popoverSource.includes('private navigateToSuggestion(markId: string | null): void {')
      && popoverSource.includes('this.clearReviewActionRetryTimer();')
      && popoverSource.includes('preventMousePointerDown = false,')
      && popoverSource.includes('preventMouseDown = false,')
      && popoverSource.includes("button.addEventListener('pointerdown', event => {")
      && popoverSource.includes("button.addEventListener('mousedown', event => {")
      && popoverSource.includes('let skipSyntheticClick = false;')
      && popoverSource.includes("if (event.pointerType === 'mouse' && (preventMousePointerDown || preventMouseDown)) {")
      && popoverSource.includes('if (skipSyntheticClick) return;')
      && popoverSource.includes('if (preventMouseDown) {')
      && popoverSource.includes('preventMousePointerDown: true,')
      && popoverSource.includes('preventMouseDown: true,')
      && popoverSource.includes('openForMark('),
    'Expected suggestion review UI to support a desktop side panel, typed suggestion badges, hover/direct review entry points, a simple-markup suggestion rail that merges adjacent changed lines into a continuous narrow rail with rounded endcaps and only labels multi-change lines, capture-phase review key handling, persisted review actions that avoid share-mode accept races, retry transient review actions, active suggestion navigation that advances after review, and first-click side-panel review buttons that suppress the desktop focus steal',
  );
  assert(
    contextMenuSource.includes('resolveSuggestionContext(')
      && contextMenuSource.includes('suggestionContext')
      && contextMenuSource.includes('data-action="accept-suggestion"')
      && contextMenuSource.includes('data-action="reject-suggestion"')
      && contextMenuSource.includes('data-action="review-suggestion"')
      && contextMenuSource.includes('data-action="show-shortcuts"')
      && contextMenuSource.includes("Keyboard shortcuts")
      && contextMenuSource.includes('showShortcutsPopover();')
      && contextMenuSource.includes('proofKeybindingConfig.acceptSuggestionAndNext')
      && contextMenuSource.includes('await proof.markAcceptPersisted(markId);')
      && contextMenuSource.includes('await proof.markRejectPersisted(markId);')
      && contextMenuSource.includes('proof.markAccept(markId);')
      && contextMenuSource.includes('proof.markReject(markId);')
      && contextMenuSource.includes('proof.navigateToMark(markId);')
      && contextMenuSource.includes('showContextMenu(view, e.clientX, e.clientY, e.target);'),
    'Expected right-click review actions to be integrated into the shared editor context menu, prefer the persisted review API, and expose a keyboard-shortcuts reveal sourced from the shared keybinding config',
  );
  assert(
    marksSource.includes('function insertAcceptNeedsMaterialization(')
      && marksSource.includes('existingText === content')
      && marksSource.includes('!insertAcceptNeedsMaterialization(content, effectiveParser)')
      && marksSource.includes('tr = removeSuggestionAnchors(tr, new Set([mark.id]));'),
    'Expected plain live insert accepts to finalize by clearing the suggestion anchor instead of reapplying already-visible text',
  );
  assert(
    editorSource.includes("return normalized.length > 0 && normalized !== 'Saved';")
      && editorSource.includes("this.shareBannerSyncLabelEl.style.opacity = shouldShowText ? '1' : '0';")
      && editorSource.includes('min-width:78px;')
      && editorSource.includes('min-width:52px;'),
    'Expected the share banner sync status to stay layout-stable and stop flashing Saved on every small interaction',
  );
  assert(
    editorSource.includes('container.dataset.humanPresenceSignature === signature')
      && editorSource.includes("container.dataset.humanPresenceDisplay === nextDisplay")
      && editorSource.includes('this.shareHumanPresenceSignature = signature;')
      && editorSource.includes("map((agent) => `${agent.id}:${agent.status}:${typeof agent.name === 'string' ? agent.name : ''}`)"),
    'Expected collaborator and agent banner controls to skip rebuilds when only heartbeat noise changes',
  );
  assert(
    editorSource.includes('private scheduleShareMarksFlush(): void {')
      && editorSource.includes('this.shareMarksFlushTimer = setTimeout(() => {')
      && editorSource.includes('this.flushShareMarks();'),
    'Expected share mode mark sync to defer flushes until the next tick',
  );
  assert(
    editorSource.includes('private pendingDomSuggestionSelection: MarkRange | null = null;')
      && editorSource.includes("const domSelectionRange = this.pendingDomSuggestionSelection ?? this.getDomSelectionRange(view);")
      && editorSource.includes('const clearPendingDomSuggestionSelection = () => {')
      && editorSource.includes('clearPendingDomSuggestionSelection();')
      && editorSource.includes("tr.setMeta('proof-dom-selection-range', domSelectionRange);")
      && editorSource.includes('this.pendingDomSuggestionSelection = null;'),
    'Expected tracked native overwrites to preserve the pre-input DOM selection long enough for suggestion wrapping while clearing stale DOM ranges before review, load, remote, and undo transactions can leak them into later edits',
  );
  assert(
    editorSource.includes("if (source === 'review-backfill') return true;")
      && editorSource.includes("this.publishProjectionMarkdown(view, projectionMarkdown, 'review-backfill');"),
    'Expected share review retries to republish the live projection markdown before retrying accept/reject',
  );
  assert(
    editorSource.includes('private syncShareCollabStateFromView(')
      && editorSource.includes('const metadata = getMarkMetadataWithQuotes(view.state);')
      && editorSource.includes('collabClient.syncEditorState(view.state.doc, this.normalizeMarkdownForCollab(markdown, metadata));')
      && editorSource.includes('collabClient.setMarksMetadata(metadata);'),
    'Expected share review actions to force-sync the live editor state back into collab using the same semantic suggestion metadata that drives the visible text',
  );
  assert(
    editorSource.includes('private normalizeMarkdownForCollab(markdown: string, marks?: Record<string, StoredMark>): string {')
      && editorSource.includes('stripAllProofSpanTagsWithReplacements(')
      && editorSource.includes('function buildCollabProofSpanReplacementMap(marks: Record<string, StoredMark>): Record<string, string> {')
      && editorSource.includes("if (kind === 'insert' || kind === 'replace') {")
      && editorSource.includes("if (kind === 'delete') {")
      && editorSource.includes('buildCollabProofSpanReplacementMap(marks),')
      && editorSource.includes('return stripProofSpanTags(runtimeMarkdown);'),
    'Expected collab markdown normalization to rebuild semantic live text from exact suggestion content when live suggestion metadata is available',
  );
  assert(
    editorSource.includes('private notifyAuthorshipStatsUpdated(stats: ReturnType<typeof getAuthorshipStats>): void {')
      && editorSource.includes("if (!bridge || typeof bridge.authorshipStatsUpdated !== 'function') return;")
      && !editorSource.includes('this.bridge.authorshipStatsUpdated(stats);'),
    'Expected authorship bridge notifications to be guarded so web share pages do not throw on review actions',
  );
  const handleMarksChangeBlock = sliceBetween(editorSource, '  private handleMarksChange(', '\n  private serializeMarkdown(');
  assert(
    handleMarksChangeBlock.includes('if (this.isShareMode) {')
      && handleMarksChangeBlock.includes('if (this.collabEnabled && this.collabCanEdit && this.editor) {')
      && handleMarksChangeBlock.includes('const serializer = this.editor.ctx.get(serializerCtx);')
      && handleMarksChangeBlock.includes('this.syncShareCollabStateFromView(view, serializer);')
      && handleMarksChangeBlock.includes('this.scheduleShareMarksFlush();')
      && handleMarksChangeBlock.includes('} else if (this.collabEnabled && this.collabCanEdit) {')
      && handleMarksChangeBlock.includes('collabClient.setMarksMetadata(metadata);'),
    'Expected share mode mark updates to sync the live editor projection back into collab immediately while still deferring the share mark flush',
  );

  const rejectSuggestionBlock = sliceBetween(editorSource, '  rejectSuggestion(id: string): boolean {', '\n  /**');
  assert(rejectSuggestionBlock.includes('return this.markReject(String(id));'), 'Expected rejectSuggestion to delegate to markReject');

  const acceptAllBlock = sliceBetween(editorSource, '  acceptAllSuggestions(): number {', '\n  /**');
  assert(acceptAllBlock.includes('return this.markAcceptAll();'), 'Expected acceptAllSuggestions to delegate to markAcceptAll');

  const rejectAllBlock = sliceBetween(editorSource, '  rejectAllSuggestions(): number {', '\n  /**');
  assert(rejectAllBlock.includes('return this.markRejectAll();'), 'Expected rejectAllSuggestions to delegate to markRejectAll');

  const markAcceptBlock = sliceBetween(editorSource, '  markAccept(markId: string): boolean {', '\n  /**\n   * Reject a suggestion without changing the document\n   */');
  assert(
    markAcceptBlock.includes('success = acceptMark(view, markId, parser);')
      && markAcceptBlock.includes('if (success && this.isShareMode) {')
      && markAcceptBlock.includes('const metadata = getMarkMetadataWithQuotes(view.state);')
      && markAcceptBlock.includes('this.lastReceivedServerMarks = { ...metadata };')
      && markAcceptBlock.includes('this.initialMarksSynced = true;')
      && markAcceptBlock.includes('void shareClient.acceptSuggestion(markId, actor).then((result) => {')
      && markAcceptBlock.includes('const serverMarks = this.sanitizeFinalizedSuggestionServerMarks(')
      && markAcceptBlock.includes("console.error('[markAccept] Failed to persist suggestion acceptance via share mutation:', error);"),
    'Expected markAccept to apply locally first in share mode, sanitize stale server suggestion payloads, and then persist through the share mutation route',
  );

  const markAcceptPersistedBlock = sliceBetween(editorSource, '  async markAcceptPersisted(markId: string): Promise<boolean> {', '\n  async markRejectPersisted(');
  assert(
    markAcceptPersistedBlock.includes('const result = await shareClient.acceptSuggestion(markId, actor);')
      && markAcceptPersistedBlock.includes('this.flushShareMarks({ keepalive: true });')
      && markAcceptPersistedBlock.includes('const backfillPlan = this.getShareSuggestionBackfillPlan(markId, result);')
      && markAcceptPersistedBlock.includes("const retriedResult = await this.retryShareSuggestionMutationAfterSync(markId, actor, 'accept', result);")
      && markAcceptPersistedBlock.includes('const effectiveBackfillPlan = this.getShareSuggestionBackfillPlan(markId, retriedResult) ?? backfillPlan;')
      && markAcceptPersistedBlock.includes('await this.backfillMissingShareSuggestionMetadata(effectiveBackfillPlan, actor)')
      && markAcceptPersistedBlock.includes('const followupResult = effectiveBackfillPlan')
      && markAcceptPersistedBlock.includes("const effectiveResult = await this.retryShareSuggestionMutationAfterSync(")
      && markAcceptPersistedBlock.includes('const serverMarks = this.sanitizeFinalizedSuggestionServerMarks(')
      && markAcceptPersistedBlock.includes('success = acceptMark(view, markId, parser);')
      && markAcceptPersistedBlock.includes('this.syncShareCollabStateFromView(view, serializer);')
      && markAcceptPersistedBlock.includes('pruneMissingSuggestions: true'),
    'Expected markAcceptPersisted to retry fragment-divergence/stale-base races both before and after any share suggestion metadata backfill, sanitize stale resolved suggestions, and then prune resolved suggestions from the local share-mode UI',
  );

  const markRejectPersistedBlock = sliceBetween(editorSource, '  async markRejectPersisted(markId: string): Promise<boolean> {', '\n  /**\n   * Accept all pending suggestions\n   */');
  assert(
    markRejectPersistedBlock.includes('const result = await shareClient.rejectSuggestion(markId, actor);')
      && markRejectPersistedBlock.includes('this.flushShareMarks({ keepalive: true });')
      && markRejectPersistedBlock.includes('const backfillPlan = this.getShareSuggestionBackfillPlan(markId, result);')
      && markRejectPersistedBlock.includes("const retriedResult = await this.retryShareSuggestionMutationAfterSync(markId, actor, 'reject', result);")
      && markRejectPersistedBlock.includes('const effectiveBackfillPlan = this.getShareSuggestionBackfillPlan(markId, retriedResult) ?? backfillPlan;')
      && markRejectPersistedBlock.includes('await this.backfillMissingShareSuggestionMetadata(effectiveBackfillPlan, actor)')
      && markRejectPersistedBlock.includes('const followupResult = effectiveBackfillPlan')
      && markRejectPersistedBlock.includes("const effectiveResult = await this.retryShareSuggestionMutationAfterSync(")
      && markRejectPersistedBlock.includes('const serverMarks = this.sanitizeFinalizedSuggestionServerMarks(')
      && markRejectPersistedBlock.includes('success = rejectMark(view, markId);')
      && markRejectPersistedBlock.includes('this.syncShareCollabStateFromView(view, serializer);')
      && markRejectPersistedBlock.includes('pruneMissingSuggestions: true'),
    'Expected markRejectPersisted to retry fragment-divergence/stale-base races both before and after any share suggestion metadata backfill, sanitize stale resolved suggestions, and then prune resolved suggestions from the local share-mode UI',
  );
  const updateShareEditGateBlock = sliceBetween(editorSource, '  private updateShareEditGate(): void {', '\n  private ensureShareWebSocketConnection(): void {');
  assert(
    editorSource.includes('private sanitizeFinalizedSuggestionServerMarks(')
      && editorSource.includes('delete sanitized[markId];')
      && updateShareEditGateBlock.includes('const baseAllowLocalEdits = this.collabEnabled')
      && updateShareEditGateBlock.includes('&& this.collabCanEdit')
      && !updateShareEditGateBlock.includes("&& this.collabConnectionStatus === 'connected'")
      && !updateShareEditGateBlock.includes('&& this.collabIsSynced')
      && updateShareEditGateBlock.includes('const gateChanged = this.shareAllowLocalEdits !== allowLocalEdits;')
      && updateShareEditGateBlock.includes('const filterChanged = this.shareContentFilterEnabled !== nextContentFilterEnabled;')
      && updateShareEditGateBlock.includes('if (!gateChanged && !filterChanged) return;')
      && editorSource.includes('private lastAppliedEditableState: boolean | null = null;'),
    'Expected share editing to stay enabled through transient sync noise and stale server marks to be stripped before they can resurrect resolved suggestions',
  );
  assert(
    suggestionsSource.includes('function findReusableInsertSuggestionCandidate(')
      && suggestionsSource.includes('const reusableInsert = findReusableInsertSuggestionCandidate(')
      && suggestionsSource.includes('const synced = syncEditableSuggestionMetadata(metadata, newTr.doc, suggestionType, reusableInsert);')
      && suggestionsSource.includes('lastInsertByActor.set(actor, {'),
    'Expected replacements inside pending insertions to reuse the existing insert suggestion id instead of fragmenting into a second suggestion that needs another accept',
  );
  assert(
    editorSource.includes('private getShareSuggestionBackfillPlan(')
      && editorSource.includes("result.error.code === 'MARK_REHYDRATION_INCOMPLETE'")
      && editorSource.includes("result.error.code === 'MARK_NOT_HYDRATED'")
      && editorSource.includes('return { markIds: missingMarkIds.length > 0 ? missingMarkIds : null };')
      && editorSource.includes('metadata = entries.length > 0 ? Object.fromEntries(entries) : null;')
      && editorSource.includes('expectedQuotes = Object.fromEntries(')
      && editorSource.includes('const serialized = this.normalizeMarkdownForRuntime(serializer(view.state.doc));')
      && editorSource.includes('projectionMarkdown = extractMarks(serialized).content;')
      && editorSource.includes('collabProjectionMarkdown = projectionMarkdown')
      && editorSource.includes('const collabProjectionCanSatisfyExpectedQuotes = !collabProjectionMarkdown')
      && editorSource.includes('&& collabProjectionCanSatisfyExpectedQuotes')
      && editorSource.includes("this.publishProjectionMarkdown(view, projectionMarkdown, 'review-backfill');")
      && editorSource.includes('collabClient.setMarksMetadata(metadata as Record<string, StoredMark>);')
      && editorSource.includes('synced = await this.waitForShareSuggestionMetadata(markIds, expectedQuotes);')
      && editorSource.includes('const pushed = await shareClient.pushUpdate(')
      && editorSource.includes('private async waitForShareSuggestionMetadata(')
      && editorSource.includes('expectedQuotes: Record<string, string> = {}')
      && editorSource.includes('return !expected || content.includes(expected);'),
    'Expected share suggestion persistence to skip dead-end collab quote waits when the semantic projection intentionally hides the missing quote, then persist the live markdown plus marks and wait for the live quote to exist server-side before retrying',
  );

  const navigateNextSuggestionBlock = sliceBetween(editorSource, '  navigateToNextSuggestion(): string | null {', '\n  /**\n   * Navigate to the previous pending suggestion\n   */');
  assert(
    navigateNextSuggestionBlock.includes('const activeId = getActiveMarkId(view.state);')
      && navigateNextSuggestionBlock.includes('const activeIndex = activeId')
      && navigateNextSuggestionBlock.includes('const baseIndex = activeIndex >= 0 ? activeIndex : this.currentSuggestionIndex;'),
    'Expected next-suggestion navigation to derive its starting point from the active suggestion when available',
  );

  const navigatePrevSuggestionBlock = sliceBetween(editorSource, '  navigateToPrevSuggestion(): string | null {', '\n  resolveActiveComment(): boolean {');
  assert(
    navigatePrevSuggestionBlock.includes('const activeId = getActiveMarkId(view.state);')
      && navigatePrevSuggestionBlock.includes('const activeIndex = activeId')
      && navigatePrevSuggestionBlock.includes('const baseIndex = activeIndex >= 0 ? activeIndex : this.currentSuggestionIndex;'),
    'Expected previous-suggestion navigation to derive its starting point from the active suggestion when available',
  );

  const markAcceptAllBlock = sliceBetween(editorSource, '  markAcceptAll(): number {', '\n  /**\n   * Reject all pending suggestions\n   */');
  assert(
    markAcceptAllBlock.includes('acceptedIds = getPendingSuggestions(getMarks(view.state)).map((mark) => mark.id);')
      && markAcceptAllBlock.includes('const result = await shareClient.acceptSuggestion(suggestionId, actor);'),
    'Expected markAcceptAll to persist each accepted suggestion through share mutations',
  );

  assert(
    shareClientSource.includes('async acceptSuggestion(')
      && shareClientSource.includes("/agent/${encodeURIComponent(this.slug)}/marks/accept"),
    'Expected ShareClient to expose a dedicated acceptSuggestion mutation',
  );
  assert(
    shareClientSource.includes('missingMarkIds?: string[];')
      && shareClientSource.includes('const missingMarkIds = Array.isArray(body.missingMarkIds)')
      && shareClientSource.includes('...(missingMarkIds.length > 0 ? { missingMarkIds } : {}),'),
    'Expected ShareClient request errors to preserve missingMarkIds for share review retries',
  );

  const acceptRouteBlock = sliceBetween(
    agentRoutesSource,
    "agentRoutes.post('/:slug/marks/accept', async (req: Request, res: Response) => {",
    "\nagentRoutes.post('/:slug/marks/reject',",
  );
  assert(
    acceptRouteBlock.includes('const collabStatus = await notifyCollabMutation(')
      && acceptRouteBlock.includes('verify: true')
      && acceptRouteBlock.includes("source: 'marks.accept'")
      && acceptRouteBlock.includes('stabilityMs: SUGGESTION_COLLAB_STABILITY_MS')
      && acceptRouteBlock.includes("const canTreatCommittedAcceptAsVerified = !collabStatus.confirmed")
      && acceptRouteBlock.includes("collabStatus.reason === 'markdown_mismatch'")
      && acceptRouteBlock.includes('collabStatus.fragmentConfirmed === true')
      && acceptRouteBlock.includes('collabStatus.canonicalConfirmed !== false')
      && acceptRouteBlock.includes('if (canTreatCommittedAcceptAsVerified) {')
      && acceptRouteBlock.includes("code: 'COLLAB_SYNC_FAILED'"),
    'Expected /marks/accept to use the shorter suggestion-specific collab stability window while still tolerating committed insertion accepts when only markdown verification is lagging',
  );
  assert(
    editorSource.includes('private hasRecentLocalCollabEditingActivity(now = Date.now()): boolean {')
      && editorSource.includes('this.pendingProjectionPublish')
      && editorSource.includes('this.contentSyncTimeout !== null')
      && editorSource.includes('(now - this.lastLocalTypingAt) < this.collabTypingRecoveryGraceMs')
      && editorSource.includes('if (this.collabCanEdit && this.shouldPreservePendingLocalCollabState()) {'),
    'Expected collab recovery to defer reconnects while recent local editing activity is still in flight',
  );
  assert(
    markRehydrationSource.includes("id.startsWith('serialized-authored:') || id.startsWith('authored:')")
      && markRehydrationSource.includes('function isPreservableSuggestionHydrationGap(')
      && markRehydrationSource.includes('const preservableSuggestionGapIds = rehydrated.missingRequiredIds.filter(')
      && markRehydrationSource.includes('const repairedMarks = preserveCanonicalMarks(finalized.marks, canonicalMarks, preservableSuggestionGapIds);'),
    'Expected structured review rehydration to ignore authored hydration-only gaps and preserve unrelated pending suggestions instead of blocking review actions',
  );

  console.log('✓ suggestion API actions route through share-aware accept/reject persistence');
}

try {
  run();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
