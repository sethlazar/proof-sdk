# Track Changes Visible UI Test

## Purpose

This is the canonical browser test for the current remaining `Simple markup` reliability bug.

The test must be run against the visible browser UI, not a hidden browser, not a direct JS API call, and not a backend-only accept path.

The point of the test is to prove that the right-hand rail and side-panel `Accept` flow behaves correctly when used exactly the way the user uses it.

## Hard Rules

- Use a fresh doc every time.
- Use a brand-new cacheless Chrome profile every time.
- If the browser shows a login / anonymous gate, click through it in the same visible window you will use for the test.
- The agent must operate on the same visible browser session the user could look at.
- The critical action must be a literal UI interaction:
  - click the right-hand rail
  - wait for the side review panel to visibly open
  - click the visible `Accept` button in that panel
- Do not substitute direct `window.proof.markAcceptPersisted(...)` calls for the critical accept step.
- Do not count a test as passed unless the browser rendering and the persisted server state both pass.

## Current Repro Target

The current bug is not just "accept is broken" in the abstract.

The live UI path can do one of three bad things:

- `Accept` is inert: the side-panel click does not reduce the pending suggestion count.
- `Accept` causes the document to collapse into a giant pending insert containing repeated copies of earlier sentences.
- Editing immediately after a tracked delete can drop typed text unless the user first moves to a fresh line.

This test is specifically designed to expose the second failure in a visible, repeatable way.

## Exact UI Test

### Setup

1. Create a fresh share doc with exactly:

```text
Alpha beta gamma.
```

2. Open the tokenized share URL in a brand-new Chrome profile with:

- cache disabled
- no first-run dialogs
- no default-browser dialog
- remote debugging enabled so the same visible session can be observed

3. Confirm the browser tab title is distinctive.

Recommended title:

- `MINTY VISIBLE SHARED REPRO`

4. If the page shows `Continue anonymously`, click it in that same visible window.

5. Confirm the visible editor is loaded before doing anything else.

### Repro Sequence

All steps below must be performed in the same visible browser window.

1. Type a normal untracked sentence:

```text
Minty typed this line in the visible repro window.
```

2. Turn on `Track Changes` from the top toggle.

3. Select the preceding plain sentence and press delete/backspace.

Expected intermediate state:

- a pending delete suggestion exists for that sentence
- the deleted text is hidden in simple markup
- the subtle delete indicator appears

4. Immediately type a new sentence at the same location:

```text
Minty added this new tracked sentence.
```

This currently may fail to stick. That is a real bug signal and should be logged if it happens.

5. Move to a fresh new line and type:

```text
Minty wrote a fresh sentence on a new line.
```

Expected intermediate state:

- a pending insert suggestion exists for this sentence

6. Move to another fresh new line and type:

```text
Minty clicked to accept this new tracked sentence.
```

Expected intermediate state:

- a second pending insert suggestion exists
- the sentence is shown inline in green simple-markup text
- the right-hand rail shows a pending change marker for the line

7. Click the right-hand rail marker for the latest sentence.

Success at this stage means:

- the side review panel visibly opens
- the panel is visibly associated with that change
- a visible `Accept` button is present in the panel

8. Click the visible `Accept` button with the mouse.

This is the critical action. Do not replace it with a programmatic accept.

## What Counts As Success

After the visible panel `Accept` click, all of the following must be true:

1. The accepted sentence:

```text
Minty clicked to accept this new tracked sentence.
```

remains in the document as ordinary accepted text.

2. That sentence is no longer pending:

- its pending suggestion disappears
- its rail marker disappears if it was the only change on that line
- it is no longer green pending text

3. Other unrelated pending suggestions remain stable:

- the earlier delete stays a delete unless explicitly accepted
- the earlier insert on the other line stays pending unless explicitly accepted

4. The document does not duplicate earlier paragraphs.

In particular, this must not happen:

- `Alpha beta gamma.` repeated many times
- `Minty wrote a fresh sentence on a new line.` repeated many times
- `Minty clicked to accept this new tracked sentence.` repeated many times
- one giant insert suggestion spanning repeated prior text blocks

5. The visible editor remains usable:

- no major flicker loop
- no cursor theft
- no jump to `BODY` with typing going nowhere

6. The persisted server state matches the visible document.

Check:

- `GET /api/agent/:slug/state`

The canonical state must not contain a giant repeated insert or duplicated paragraphs.

## What Currently Counts As Failure

Any one of these means the test failed:

- clicking the rail does not visibly open the side panel
- clicking visible `Accept` does nothing
- suggestion count does not decrease when it should
- the wrong suggestion is accepted
- the doc temporarily or permanently duplicates large blocks of prior content
- the accepted sentence becomes part of one giant insert containing older text
- typed text disappears after a tracked delete
- server state diverges from what the browser appears to show

## Current Observed Failure From The Visible UI

The strongest current repro from the shared visible window is:

1. The doc has a pending insert:

```text
Minty clicked to accept this new tracked sentence.
```

2. A new pending insert is added:

```text
Minty reproduced the rail accept glitch again.
```

3. The rail is clicked.

4. The side panel opens and the visible `Accept` button appears.

5. `Accept` is clicked in the visible panel.

6. The document collapses into one giant pending insert containing repeated copies of:

- `Alpha beta gamma.`
- `Minty wrote a fresh sentence on a new line.`
- `Minty clicked to accept this new tracked sentence.`
- `Minty reproduced the rail accept glitch again.`

This is the key UI-path failure to fix.

## Logging Requirements

Every agent run must append a short entry to:

- `docs/track-changes-automation-log.md`

Each entry should include:

- date/time
- agent / branch / commit
- fresh doc URL
- exact visible UI sequence attempted
- result
- whether the visible panel opened
- whether `Accept` changed the intended suggestion
- whether duplication occurred
- whether server state matched the browser
- next hypothesis

## Recommendation To Future Agents

If a lower-level accept path works while the visible rail + side-panel path fails, prefer to preserve the underlying suggestion engine and make the UI path call the same successful path.

Do not treat this as evidence that the whole suggestion backend is wrong. Treat it as evidence that the visible review UI is still dispatching or sequencing work incorrectly.
