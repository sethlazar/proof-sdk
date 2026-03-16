import { readFileSync } from 'node:fs';
import path from 'node:path';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

function run(): void {
  const source = readFileSync(path.resolve(process.cwd(), 'src/bridge/collab-client.ts'), 'utf8');

  assert(
    source.includes('reconnectWithSession(session: CollabSessionInfo, options?: { preserveLocalState?: boolean }): void'),
    'Expected collab runtime to expose reconnectWithSession',
  );
  assert(
    source.includes('getYDoc(): Y.Doc | null'),
    'Expected collab runtime to expose getYDoc',
  );
  assert(
    source.includes('getAwareness(): Awareness | null'),
    'Expected collab runtime to expose getAwareness',
  );
  assert(
    source.includes('setProjectionMarkdown(markdown: string): void'),
    'Expected collab runtime to expose projection markdown writes',
  );
  assert(
    source.includes('setMarksMetadata(marks: Record<string, unknown>): void'),
    'Expected collab runtime to expose marks metadata writes',
  );
  assert(
    source.includes('setMarksMetadata(')
      && source.includes('options?: { excludeMarkIds?: Iterable<string> | null },')
      && source.includes('const excludedMarkIds = new Set(options?.excludeMarkIds ?? []);')
      && source.includes('delete mergedMarks[markId];')
      && source.includes('if (excludedMarkIds.has(key)) return;'),
    'Expected collab marks writes to support excluding resolving suggestion ids so local preservation does not resurrect them during persisted review actions',
  );
  assert(
    source.includes('if (session.syncProtocol !== \'pm-yjs-v1\')'),
    'Expected runtime to reject unsupported collab sync protocols',
  );
  assert(
    source.includes('provider.on(\'status\''),
    'Expected provider status event wiring',
  );
  assert(
    source.includes('provider.on(\'synced\''),
    'Expected provider synced event wiring',
  );
  assert(
    source.includes('provider.on(\'unsyncedChanges\''),
    'Expected provider unsyncedChanges event wiring',
  );
  assert(
    source.includes('provider.on(\'close\''),
    'Expected provider close event wiring',
  );
  assert(
    source.includes('provider.on(\'authenticationFailed\'')
      && source.includes('lastAuthenticationFailureReason')
      && !source.includes('mapAuthFailureToTerminalReason'),
    'Expected auth failures to remain refreshable signals instead of immediate terminal close handling',
  );
  assert(
    source.includes('preserveConnection: false'),
    'Expected provider to disable preserveConnection so auth failures fully tear down stale sockets',
  );
  assert(
    source.includes('private activeSession: CollabSessionInfo | null = null;')
      && source.includes('token: () => this.activeSession?.token ?? null,'),
    'Expected provider auth to read from the live session token instead of a fixed initial token',
  );
  assert(
    source.includes('requiresHardReconnect(session: CollabSessionInfo): boolean {')
      && source.includes('softRefreshSession(session: CollabSessionInfo): boolean {')
      && source.includes('this.provider.setConfiguration({')
      && source.includes('this.provider.configuration.websocketProvider.setConfiguration({')
      && source.includes('this.provider.disconnect();')
      && source.includes('void this.provider.connect();'),
    'Expected collab runtime to support soft session refresh on the existing provider/Y.Doc before falling back to hard reconnect',
  );
  assert(
    source.includes('const preserveLocalState = options?.preserveLocalState !== false;')
      && source.includes('private hasPendingLocalStateForReconnect(): boolean {')
      && source.includes('return this.unsyncedChanges > 0 || this.durablePendingUpdates.length > 0;')
      && source.includes('const canPreserveLocalState = preserveLocalState')
      && source.includes('&& this.canPersistDurableUpdates(session.role)')
      && source.includes('&& this.hasPendingLocalStateForReconnect();')
      && source.includes('const localState = canPreserveLocalState && this.ydoc ? Y.encodeStateAsUpdate(this.ydoc) : null;')
      && source.includes("Y.applyUpdate(this.ydoc, localState, 'local-reconnect-bootstrap');"),
    'Expected reconnect path to preserve local Yjs state only for writable roles with real pending local state',
  );
  assert(
    source.includes('this.activeSession.accessEpoch === session.accessEpoch;'),
    'Expected hard-reconnect decisions to include accessEpoch changes',
  );
  assert(
    source.includes('private sessionRole: ShareRole | null = null;')
      && source.includes("if (!this.sessionRole || !this.canPersistDurableUpdates(this.sessionRole)) {"),
    'Expected collab runtime to hard-stop projection and mark writes for read-only roles',
  );
  assert(
    source.includes("if (transaction.origin === 'local-marks-sync') return;"),
    'Expected marks map listener to ignore local marks transactions',
  );
  assert(
    source.includes('DURABLE_UPDATE_KEY_PREFIX')
      && source.includes('proof:collab:pending-updates:')
      && source.includes('localStorage'),
    'Expected collab runtime to persist durable local updates',
  );
  assert(
    source.includes("return role === 'editor' || role === 'owner_bot';")
      && source.includes('this.durableUpdatesEnabled = this.canPersistDurableUpdates(session.role);'),
    'Expected durable buffering/replay to be limited to edit-capable roles',
  );
  assert(
    source.includes('replayDurableUpdates')
      && source.includes('durable-replay'),
    'Expected collab runtime to replay buffered updates on reconnect',
  );

  assert(
    !source.includes('onSnapshot('),
    'Did not expect legacy snapshot subscription API',
  );
  assert(
    !source.includes('setLocalSnapshot('),
    'Did not expect legacy snapshot write API',
  );
  assert(
    !source.includes('onConflict('),
    'Did not expect legacy conflict callback API',
  );
  assert(
    !source.includes('reconcileSnapshots('),
    'Did not expect reconcileSnapshots usage in collab runtime',
  );

  console.log('✓ milkdown collab runtime lifecycle + transport contract');
}

try {
  run();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
