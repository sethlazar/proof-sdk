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
      && keybindingsSource.includes("'Mod-Shift-e': toggleSuggestionsCommand"),
    'Expected dedicated keyboard shortcuts for accepting, rejecting, navigating, and toggling suggestions',
  );
  assert(
    editorSource.includes('private createTrackChangesModeToggle(): HTMLElement {')
      && editorSource.includes("makeSegment('Edit'")
      && editorSource.includes("makeSegment('Track Changes'"),
    'Expected the share banner to expose a visible Edit / Track Changes toggle',
  );
  assert(
    popoverSource.includes("view.dom.addEventListener('mousemove', this.handleEditorMouseMove);")
      && popoverSource.includes("source?: 'direct' | 'hover'")
      && popoverSource.includes("appendDetailRow('Original text', original);"),
    'Expected suggestion popovers to open on hover and show the original replacement text',
  );
  assert(
    editorSource.includes('private scheduleShareMarksFlush(): void {')
      && editorSource.includes('this.shareMarksFlushTimer = setTimeout(() => {')
      && editorSource.includes('this.flushShareMarks();'),
    'Expected share mode mark sync to defer flushes until the next tick',
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
    markAcceptBlock.includes('void shareClient.acceptSuggestion(markId, actor).then((result) => {')
      && markAcceptBlock.includes("console.error('[markAccept] Failed to persist suggestion acceptance via share mutation:', error);"),
    'Expected markAccept to persist accepted suggestions through the share mutation route',
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

  console.log('✓ suggestion API actions route through share-aware accept/reject persistence');
}

try {
  run();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
