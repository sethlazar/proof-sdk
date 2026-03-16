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
