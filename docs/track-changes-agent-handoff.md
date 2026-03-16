# Track Changes Agent Handoff

## Mission

We are turning Proof SDK's suggestion system into a Word-like track-changes experience, with `Simple markup` as the main mode.

The user goal is narrow and concrete:

- While editing, the document should look as though edits have been accepted.
- Pending changes must still remain pending for another reviewer.
- Review should happen from a right-side rail and side panel, not a bottom sheet or hover-only bubble.
- The system must feel fast, stable, and boring in the best sense. No flicker, no focus theft, no mysterious second click, no state drift.

This branch already contains a large amount of working UI and behavior. The remaining work is reliability under real browser editing, especially after longer tracked-edit sessions.

## Key Discovery: Preserve The Old Suggestion Engine

Proof already had a substantial tracked-suggestions system before the new `Simple markup` UI work. That older approach is not dead weight. It is the main thing to preserve and harness.

The guiding rule for all agents should be:

- Keep the pre-existing suggestion and accept/reject machinery under the hood as far as possible.
- Treat this project primarily as a UI and interaction redesign, not a backend reinvention.
- Prefer making the old suggestion engine drive the new rail, panel, and simple-markup views.
- Only replace core suggestion semantics if there is clear evidence the old approach cannot support the required user experience.

In practice, this means the target is:

- old suggestion architecture underneath
- new Word-like `Simple markup` UI on top

Do not drift into building a second competing review model unless you can justify it with concrete evidence.

## Branch And Starting Point

Work from the current branch:

- `codex/simple-markup-track-changes`

Do not start by editing this worktree directly. Make a fresh worktree and branch from the current branch head so multiple agents can work in parallel safely.

## Required Setup For Each Agent

### 1. Create a fresh worktree first

From the main local clone:

```bash
cd /Volumes/Agents/Active-Research/Local-Repos/proof-sdk
git fetch origin
git worktree add -b codex/<agent-name>-track-changes ../proof-sdk-<agent-name> origin/codex/simple-markup-track-changes
cd ../proof-sdk-<agent-name>
```

Example:

```bash
git worktree add -b codex/hegel-track-changes ../proof-sdk-hegel origin/codex/simple-markup-track-changes
```

### 2. Install and run locally

```bash
npm install
npm run build
npm run serve
```

If you want the Vite app too:

```bash
npm run dev
```

Useful local URLs:

- Share server: `http://127.0.0.1:4000`
- Dev app: `http://127.0.0.1:3000`

The share server already serves the built web bundle and the HTML shell with `Cache-Control: no-store`, so the share path is the main verification path.

### 3. Create a fresh repro document

Use the local API, not a reused doc.

```bash
DOC_JSON=$(curl -s -X POST http://127.0.0.1:4000/documents \
  -H 'Content-Type: application/json' \
  -d '{"markdown":"Alpha beta gamma.","title":"Track changes repro"}')

echo "$DOC_JSON"
```

The response includes:

- `slug`
- `accessToken`
- `tokenUrl`

Extract the tokenized share URL:

```bash
DOC_URL=$(node -e 'const payload = JSON.parse(process.argv[1]); console.log(payload.tokenUrl);' "$DOC_JSON")
echo "$DOC_URL"
```

Use `DOC_URL` for browser testing.

### 4. Launch a fresh no-cache Chrome with remote debugging

Use a brand-new profile every time you want to verify a UI change. Do not reuse old tabs.

```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --user-data-dir="/tmp/proof-track-changes-$$" \
  --remote-debugging-port=9242 \
  --disable-application-cache \
  --disk-cache-size=1 \
  --media-cache-size=1 \
  --no-first-run \
  --no-default-browser-check \
  "$DOC_URL"
```

If port `9242` is already in use, pick another local port.

### 5. Attach OpenClaw to that exact browser

This workspace has OpenClaw available. For a live remote-debugging browser, use the live-browser path rather than the normal managed browser profile.

```bash
openclaw browser create-profile \
  --name proof-live \
  --cdp-url http://127.0.0.1:9242 \
  --color "#00AA00"

openclaw browser --browser-profile proof-live tabs
openclaw browser --browser-profile proof-live snapshot
```

Useful commands:

- `openclaw browser --browser-profile proof-live snapshot`
- `openclaw browser --browser-profile proof-live click <ref>`
- `openclaw browser --browser-profile proof-live type <ref> "text"`
- `openclaw browser --browser-profile proof-live press Enter`
- `openclaw browser --browser-profile proof-live press Meta+Alt+A`
- `openclaw browser --browser-profile proof-live console`
- `openclaw browser --browser-profile proof-live requests`
- `openclaw browser --browser-profile proof-live evaluate --fn '() => window.proof?.getSuggestions?.()'`

## User-Visible Target Behavior

This is the product target. Do not optimize for internal elegance at the expense of this experience.

- `Simple markup` is the primary mode.
- Replacements and insertions render inline as accepted-looking text while still pending.
- Pending inserted or replacement text is green text, not green highlight.
- Pending deletions are hidden from the document body. Leave a subtle indicator in-document so there is still a review anchor.
- The right-hand rail shows where changes exist.
- Clicking the rail opens a right-side review panel.
- The review panel shows original and edited text, plus accept, reject, previous, next.
- Accept and reject must work on the first click.
- Context menu review actions should live in the existing right-click menu, not a second popup.
- Keyboard shortcuts should remain centralized in a config file and visible from the floating menu.
- Undo and redo should work from the top menu and keyboard.
- The editor must stay focused during normal typing. No cursor theft. No phantom text loss. No top-bar flicker loops.

## What Already Works On This Branch

These pieces are already implemented and should be preserved unless there is a compelling reason to rethink them.

- Word-like `Simple markup` view mode exists and is wired through the editor API.
- Shared docs default to `Simple markup`.
- Track changes can be toggled from the top bar.
- Accepted-view inline suggestions exist for inserts and replacements.
- Replacements no longer need to be fake visual previews; the edited text remains live and editable while pending.
- Deleted text is hidden in `Simple markup`, with an in-document indicator and a right-rail review path.
- Right-side rail and right-side review panel are in place.
- Bottom-sheet and dim-overlay review behavior have already been removed for desktop review.
- Accept, reject, previous, and next controls exist in the side panel.
- Review actions are also wired into the existing context menu.
- Keyboard shortcuts were centralized in `src/editor/keybindings-config.ts`.
- Undo and redo buttons exist in the header, and keyboard support exists.
- Asset caching on the local share server was hardened to avoid stale bundles during iterative testing.
- A no-cache remote-debugging Chrome workflow with OpenClaw has been proven workable on this machine.

## Key Files

If you are orienting yourself quickly, start here:

- `src/editor/index.ts`
- `src/editor/plugins/marks.ts`
- `src/editor/plugins/mark-popover.ts`
- `src/editor/plugins/suggestions.ts`
- `src/editor/plugins/keybindings.ts`
- `src/editor/keybindings-config.ts`
- `src/ui/context-menu.ts`
- `server/collab.ts`
- `server/agent-routes.ts`
- `server/proof-mark-rehydration.ts`
- `server/proof-span-strip.ts`
- `server/share-web-routes.ts`
- `server/index.ts`

## Current Known Failure

Do not assume the remaining bug is "accept button broken in general." The shorter flows often work. The failure shows up under a more realistic sequence.

As of this handoff, this exact sequence can still fail:

1. Open a fresh share doc in a fresh no-cache remote-debug Chrome.
2. Turn on `Track Changes`.
3. Type a tracked insertion, for example ` delta`.
4. Accept that change.
5. Type another three lines of content in flow, still in `Track Changes`.
6. Attempt review and acceptance again.
7. Then perform a deletion and accept that too.

The observed bad behavior has included:

- insertion accepts that appear to work once, but later review state becomes unreliable
- page hang or crash after more typing
- top menu flicker
- focus being stolen from the editor
- typed text not appearing where expected
- server state diverging from browser state
- duplicated lines persisted server-side even though the browser looked sane

One concrete reproducible symptom from browser-driven testing:

- after an initial tracked insertion is accepted, typing three more tracked lines can leave the browser looking mostly right while the canonical server markdown duplicates lines such as `line one line one` and `line two line two`

Treat the focus flicker, panel weirdness, and later accept failures as potentially related symptoms of the same deeper state-management problem.

## Important Constraint: Do Not Overfit To One Hypothesis

There are several plausible places the bug may live:

- client-side projection of live suggestions
- Yjs fragment state versus canonical markdown state
- proof-span serialization and stripping
- server-side projection derivation
- accept or reject mutation ordering
- mark rehydration after local optimistic updates
- collab reconnect or recovery loops stealing focus or replaying stale state

Do not assume the right fix is a tiny patch in the last file someone touched. Work from evidence.

## Test-Driven Development Workflow

The user explicitly wants real browser verification, not just unit tests or reasoning from source.

Use this loop:

1. Reproduce on a fresh doc in a fresh no-cache browser.
2. Record what the browser shows.
3. Record what the server believes the markdown and marks are.
4. Form a narrow hypothesis.
5. Add or update a targeted regression test if you can isolate the failure.
6. Implement the smallest credible fix.
7. Rebuild and restart the share server if needed.
8. Rerun the exact browser sequence on a fresh doc.
9. Only claim success if the browser sequence and the persisted server state both pass.

Do not trust a green unit test if the browser still drifts. Do not trust a good-looking browser if the server state is wrong.

For the current canonical visible-browser repro, read this first:

- `docs/track-changes-visible-ui-test.md`

Append each attempt here:

- `docs/track-changes-automation-log.md`

## Minimum Browser Verification Sequence

Before telling Seth something is fixed, you should personally complete this exact sequence in the remote-debug browser:

1. Create a fresh doc with `Alpha beta gamma.`
2. Open it in a new no-cache Chrome profile.
3. Turn on `Track Changes`.
4. Type ` delta`.
5. Click `Accept` once.
6. Confirm the change is fully accepted and resolved.
7. Type three new lines of text in flow.
8. Confirm the editor keeps focus and the top UI does not flicker or steal the cursor.
9. Accept those new changes.
10. Delete some text.
11. Accept the deletion.
12. Confirm the browser rendering is correct.
13. Confirm the server-side document state is also correct.

If any of those steps fails, the bug is not fixed.

## Current Stronger UI-Path Repro

The older minimum browser sequence is still useful, but the strongest current failure is more specific and must be checked through the visible UI path.

See:

- `docs/track-changes-visible-ui-test.md`

In short:

1. Use the same visible browser session the user can see.
2. Create pending insert suggestions on separate lines.
3. Click the right-hand rail for the latest one.
4. Confirm the side review panel visibly opens.
5. Click the visible `Accept` button.
6. Fail the run if the doc duplicates prior text, if `Accept` is inert, or if one giant insert absorbs older content.

## Suggested Ways To Inspect State

Browser-side:

- `window.proof.getSuggestions()`
- `window.proof.getMarkdownSnapshot()`
- OpenClaw snapshots, console logs, and request traces

Server-side:

- `GET /documents/:slug/state`
- `GET /documents/:slug/snapshot`
- server logs from `npm run serve`

Look for divergence between:

- visible editor text
- suggestion metadata
- canonical markdown persisted on the server

## Existing Regression Coverage Worth Reading

These tests already cover parts of this feature area:

- `src/tests/editor-suggestion-api-regression.test.ts`
- `src/tests/marks.test.ts`
- `src/tests/proof-mark-rehydration.test.ts`
- `src/tests/collab-proof-span-projection-regression.test.ts`
- `src/tests/replace-suggestion-api-live-replacement-regression.test.ts`

Run at least:

```bash
npx tsx src/tests/editor-suggestion-api-regression.test.ts
npx tsx src/tests/marks.test.ts
npm run build
```

Run broader tests if your change touches server persistence or routing.

## Existing UX Decisions To Preserve Unless You Have A Better Verified Replacement

- Keep `Simple markup` as the main mode.
- Keep the original suggestion engine as the primary backend path wherever possible, and bend the UI around it rather than replacing it.
- Keep the right-side rail and side panel review flow.
- Keep the integrated context-menu review actions.
- Keep keyboard shortcuts centralized in config.
- Keep the keybindings reveal in the floating menu.
- Keep the improved continuous rail visuals.
- Keep the no-cache share behavior for local iteration.

## Non-Goals

Do not spend time on feature expansion unrelated to the current reliability issue.

- No new AI-writing surfaces.
- No redesign of the whole app chrome.
- No speculative refactor unless it is the cleanest path to the actual bug.
- No reverting the user-visible improvements that are already working well.

## Practical Notes

- Ignore `proof.db`. It is a local runtime artifact and should not be committed.
- If you open a browser for Seth to verify, open a brand-new cacheless window every time.
- If you think a fix works, prove it with the exact browser sequence above before handing off.
