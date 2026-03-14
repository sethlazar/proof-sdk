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
  const keybindingsSource = readFileSync(path.resolve(process.cwd(), 'src/editor/plugins/keybindings.ts'), 'utf8');
  const popoverSource = readFileSync(path.resolve(process.cwd(), 'src/editor/plugins/mark-popover.ts'), 'utf8');
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
      && setTrackChangesViewModeBlock.includes('this.trackChangesViewMode = setSuggestionDisplayMode(view, nextMode);'),
    'Expected setTrackChangesViewMode to persist and dispatch the marks plugin display mode',
  );
  assert(
    editorSource.includes("addModeItem('No markup'")
      && editorSource.includes("addModeItem('Original'"),
    'Expected the share menu to expose the extra Word-style track-changes modes',
  );
  assert(
    keybindingsSource.includes("'Mod-Alt-a': acceptActiveSuggestionCommand")
      && keybindingsSource.includes("'Mod-Alt-r': rejectActiveSuggestionCommand")
      && keybindingsSource.includes("'Mod-Alt-]': navigateNextSuggestionCommand")
      && keybindingsSource.includes("'Mod-Alt-[': navigatePrevSuggestionCommand")
      && keybindingsSource.includes("'Mod-Shift-e': toggleSuggestionsCommand")
      && keybindingsSource.includes('acceptSuggestionMark(view, activeId);')
      && keybindingsSource.includes('void proof.markAcceptPersisted(activeId);')
      && keybindingsSource.includes('void proof.markRejectPersisted(activeId);'),
    'Expected dedicated keyboard shortcuts for accepting, rejecting, navigating, and toggling suggestions, with optimistic local accept updates before share persistence finishes',
  );
  assert(
    editorSource.includes('private createTrackChangesModeToggle(): HTMLElement {')
      && editorSource.includes("makeSegment('Edit'")
      && editorSource.includes("makeSegment('Track Changes'"),
    'Expected the share banner to expose a visible Edit / Track Changes toggle',
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
      && popoverSource.includes("source?: 'direct' | 'hover'")
      && popoverSource.includes("appendDetailRow('Original text', original")
      && popoverSource.includes("private renderSuggestionRail(): void {")
      && popoverSource.includes("getSuggestionDisplayMode(this.view.state) !== 'simple'")
      && popoverSource.includes("this.suggestionRail.className = 'mark-suggestion-rail';")
      && popoverSource.includes("button.className = 'mark-suggestion-rail-button';")
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
      && popoverSource.includes("this.runSuggestionReviewAction(mark.id, 'accept', nextMarkId);")
      && popoverSource.includes("this.runSuggestionReviewAction(mark.id, 'reject', nextMarkId);")
      && popoverSource.includes('const persistedAction = action === \'accept\'')
      && popoverSource.includes('const optimisticApplied = runLocalActionOnly();')
      && popoverSource.includes('return acceptSuggestion(this.view, markId);')
      && popoverSource.includes('setReviewButtonsBusy(true);')
      && popoverSource.includes('private navigateToSuggestion(markId: string | null): void {')
      && popoverSource.includes('this.clearReviewActionRetryTimer();')
      && popoverSource.includes('openForMark('),
    'Expected suggestion review UI to support a desktop side panel, typed suggestion badges, hover/direct open where appropriate, a simple-markup suggestion rail, capture-phase review key handling, optimistic local review updates while share persistence settles, retry transient review actions, and active suggestion navigation',
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
      && editorSource.includes("tr.setMeta('proof-dom-selection-range', domSelectionRange);")
      && editorSource.includes('this.pendingDomSuggestionSelection = null;'),
    'Expected tracked native overwrites to preserve the pre-input DOM selection long enough for suggestion wrapping',
  );
  assert(
    editorSource.includes("if (source === 'review-backfill') return true;")
      && editorSource.includes("this.publishProjectionMarkdown(view, projectionMarkdown, 'review-backfill');"),
    'Expected share review retries to republish the live projection markdown before retrying accept/reject',
  );
  assert(
    editorSource.includes('private syncShareCollabStateFromView(')
      && editorSource.includes('collabClient.syncEditorState(view.state.doc, this.normalizeMarkdownForCollab(markdown));')
      && editorSource.includes('collabClient.setMarksMetadata(getMarkMetadataWithQuotes(view.state));'),
    'Expected share review actions to force-sync the live editor state back into collab when markup changes without changing plain markdown',
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
      && handleMarksChangeBlock.includes('this.scheduleShareMarksFlush();')
      && handleMarksChangeBlock.includes('} else if (this.collabEnabled && this.collabCanEdit) {')
      && handleMarksChangeBlock.includes('collabClient.setMarksMetadata(metadata);'),
    'Expected share mode mark updates to be deferred instead of writing collab metadata immediately',
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
      && markAcceptBlock.includes("console.error('[markAccept] Failed to persist suggestion acceptance via share mutation:', error);"),
    'Expected markAccept to apply locally first in share mode and then persist through the share mutation route',
  );

  const markAcceptPersistedBlock = sliceBetween(editorSource, '  async markAcceptPersisted(markId: string): Promise<boolean> {', '\n  async markRejectPersisted(');
  assert(
    markAcceptPersistedBlock.includes('const result = await shareClient.acceptSuggestion(markId, actor);')
      && markAcceptPersistedBlock.includes('const backfillPlan = this.getShareSuggestionBackfillPlan(markId, result);')
      && markAcceptPersistedBlock.includes('await this.backfillMissingShareSuggestionMetadata(backfillPlan, actor)')
      && markAcceptPersistedBlock.includes('success = acceptMark(view, markId, parser);')
      && markAcceptPersistedBlock.includes('this.syncShareCollabStateFromView(view, serializer);')
      && markAcceptPersistedBlock.includes('pruneMissingSuggestions: true'),
    'Expected markAcceptPersisted to retry after hydrating missing share suggestion metadata and then prune resolved suggestions from the local share-mode UI',
  );

  const markRejectPersistedBlock = sliceBetween(editorSource, '  async markRejectPersisted(markId: string): Promise<boolean> {', '\n  /**\n   * Accept all pending suggestions\n   */');
  assert(
    markRejectPersistedBlock.includes('const result = await shareClient.rejectSuggestion(markId, actor);')
      && markRejectPersistedBlock.includes('const backfillPlan = this.getShareSuggestionBackfillPlan(markId, result);')
      && markRejectPersistedBlock.includes('await this.backfillMissingShareSuggestionMetadata(backfillPlan, actor)')
      && markRejectPersistedBlock.includes('success = rejectMark(view, markId);')
      && markRejectPersistedBlock.includes('this.syncShareCollabStateFromView(view, serializer);')
      && markRejectPersistedBlock.includes('pruneMissingSuggestions: true'),
    'Expected markRejectPersisted to retry after hydrating missing share suggestion metadata and then prune resolved suggestions from the local share-mode UI',
  );
  assert(
    editorSource.includes('private getShareSuggestionBackfillPlan(')
      && editorSource.includes("result.error.code === 'MARK_REHYDRATION_INCOMPLETE'")
      && editorSource.includes("result.error.code === 'MARK_NOT_HYDRATED'")
      && editorSource.includes('return { markIds: missingMarkIds.length > 0 ? missingMarkIds : null };')
      && editorSource.includes('metadata = entries.length > 0 ? Object.fromEntries(entries) : null;')
      && editorSource.includes('expectedQuotes = Object.fromEntries(')
      && editorSource.includes("this.publishProjectionMarkdown(view, projectionMarkdown, 'review-backfill');")
      && editorSource.includes('collabClient.setMarksMetadata(metadata as Record<string, StoredMark>);')
      && editorSource.includes('synced = await this.waitForShareSuggestionMetadata(markIds, expectedQuotes);')
      && editorSource.includes('const pushed = await shareClient.pushUpdate(')
      && editorSource.includes('private async waitForShareSuggestionMetadata(')
      && editorSource.includes('expectedQuotes: Record<string, string> = {}')
      && editorSource.includes('return !expected || content.includes(expected);'),
    'Expected share suggestion persistence to backfill the server-reported missing pending suggestions, persist the live markdown plus marks when collab drift exists, and wait for the live quote to exist server-side before retrying',
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
      && acceptRouteBlock.includes("code: 'COLLAB_SYNC_FAILED'"),
    'Expected /marks/accept to await verified collab convergence before returning success',
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
