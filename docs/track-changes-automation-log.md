# Track Changes Automation Log

Append one entry per automation run.

## Entry Template

### YYYY-MM-DD HH:MM TZ

- Agent:
- Branch:
- Commit:
- Fresh doc URL:
- Visible browser title:
- Test sequence attempted:
- Did the rail open the side panel?
- Did clicking visible `Accept` work?
- Did the suggestion count decrease correctly?
- Did any duplicated text appear?
- Did server state match the visible browser?
- Files changed:
- Tests run:
- Result:
- Next hypothesis:

## Current Baseline

### 2026-03-17 07:00 AEDT

- Agent: Codex main thread
- Branch: `codex/minty-track-changes-parallel`
- Commit: `5932979`
- Fresh doc URL: `http://127.0.0.1:4184/d/y5d1igdb?token=88bf528b-1e90-4886-a2cf-04e115429726`
- Visible browser title: `MINTY VISIBLE SHARED REPRO - Proof`
- Test sequence attempted:
  - typed visible text
  - turned on `Track Changes`
  - created pending delete
  - created pending inserts
  - clicked rail
  - clicked visible side-panel `Accept`
- Did the rail open the side panel? Yes
- Did clicking visible `Accept` work? No, not reliably
- Did the suggestion count decrease correctly? Not reliably
- Did any duplicated text appear? Yes, in one visible UI run the doc collapsed into one giant insert containing repeated prior sentences
- Did server state match the visible browser? Not established cleanly in the failing visible-UI duplicate run; this still needs to be checked immediately after reproducing
- Files changed: none during this observation-only phase
- Tests run: live visible UI repro only
- Result: failing baseline captured
- Next hypothesis:
  - the underlying accept path may still be mostly sound
  - the visible rail + side-panel path appears to sequence or dispatch accept incorrectly
  - agents should target the visible UI path first, not replace the suggestion backend wholesale

### 2026-03-17 09:57 AEDT

- Agent: Codex main thread
- Branch: `codex/minty-track-changes-parallel-20260317-095154`
- Commit: `4f3d677`
- Fresh doc URL: not created; sandboxed shell can see local Proof listeners on `:4000` and `:4184` but cannot connect to loopback HTTP, so the canonical fresh-doc browser run could not start from this environment
- Visible browser title: not run; loopback access to the visible Chrome CDP port `127.0.0.1:9242` is also blocked from this sandbox
- Test sequence attempted:
  - read the visible UI runbook, handoff brief, automation log, and prior automation memory
  - created a fresh timestamped writable worktree at `/private/tmp/proof-sdk-minty-track-changes-20260317-095154` from `codex/minty-track-changes-parallel` HEAD `cf509aa`
  - compared current branch code against the earlier unverified persisted-accept race fix
  - restored the client/collab guard so `markAcceptPersisted` excludes the resolving mark from the pre-accept flush and skips a second local `acceptMark(...)` if live collab already cleared it
  - reran source regressions and production build in the fresh worktree
  - attempted loopback access to the local Proof server and visible Chrome debug port to run the canonical rail -> side-panel Accept repro
- Did the rail open the side panel? Not run in this sandboxed shell
- Did clicking visible `Accept` work? Not run in this sandboxed shell
- Did the suggestion count decrease correctly? Not run in this sandboxed shell
- Did any duplicated text appear? Not rechecked in a visible run; the committed patch specifically targets the stale-mark republish + double-accept path that previously produced the giant combined insert failure
- Did server state match the visible browser? Not run in this sandboxed shell
- Files changed:
  - `src/bridge/collab-client.ts`
  - `src/editor/index.ts`
  - `src/tests/editor-suggestion-api-regression.test.ts`
  - `src/tests/milkdown-collab-runtime.test.ts`
- Tests run:
  - `node --import tsx src/tests/editor-suggestion-api-regression.test.ts` (pass)
  - `node --import tsx src/tests/milkdown-collab-runtime.test.ts` (pass)
  - `node --import tsx src/tests/proof-mark-rehydration.test.ts` (pass)
  - `node --import tsx src/tests/marks.test.ts` (pass)
  - `node --import tsx src/tests/collab-reliability-round1.test.ts` (pass)
  - `npm run build` (pass)
  - `curl -I http://127.0.0.1:4000` (blocked: connection refused from sandbox)
  - `curl -I http://127.0.0.1:4184` (blocked: connection refused from sandbox)
  - `curl http://127.0.0.1:9242/json/version` (blocked: connection refused from sandbox)
- Result: credible fix committed and source/build verification passed, but the canonical visible-browser rail -> side-panel `Accept` test is still unverified in this automation environment because loopback access to the local server and visible Chrome session is sandbox-blocked
- Next hypothesis:
  - verify commit `4f3d677` first in an unrestricted fresh cacheless visible Chrome session using the literal rail click and visible side-panel `Accept`
  - if the visible duplicate or inert Accept bug still reproduces after this patch, harden the server/collab accept path to reject or repair stale suggestion metadata before canonicalization instead of trusting the pre-accept Yjs fallback state
