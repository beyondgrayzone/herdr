# herdr Extension  Agent State in Herdr

> Reports Kit's agent lifecycle to a Herdr pane, so Herdr can show and act on
> what Kit is doing.

> Verified against `herdr 0.9.1-preview`.

> Still a WIP project

> This extension was created for `https://github.com/beyondgrayzone/kit` which is custom fork of `github.com/mark3labs/kit` with added events for extensions API

Herdr already understands a long list of coding agents (Claude Code, Codex,
Cursor, Droid, OpenCode, ...) and gives each pane a semantic state in its
**Agents** sidebar, plus waits, notifications and rollups keyed on that state.
Kit is not on that list, so inside a Herdr pane a running Kit looks exactly
like an idle shell.

This extension closes that gap. Once loaded, a Herdr pane running Kit reports:

```
- working    
- blocked    
- idle
```

## Table of Contents

- [Quick Start](#quick-start)
- [How Activation Works](#how-activation-works)
- [What Gets Reported](#what-gets-reported)
- [State vs. Label](#state-vs-label)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Quick Start

Clone right into the Kit extension 

```
cd ~/.config/kit/extensions
git clone https://github.com/beyondgrayzone/herdr .
```

There is nothing to install. Herdr auto-discovers this file in
`~/.config/kit/extensions/`, so:

```bash
# 1. Start Herdr
herdr

# 2. Create a pane that runs Kit
#    (any pane Herdr launches inherits the Herdr environment automatically)

# 3. Use the pane as normal
kit
```

That is the whole setup. The extension detects Herdr from the environment
Herdr injects into every pane it manages, activates itself, and stays
completely inert everywhere else.

To confirm it is live, start your kit agent and see herdr show the status 

**Load it explicitly** instead of relying on discovery:

```bash
kit --no-extensions -e herdr.go
```

## How Activation Works

The extension registers **no handlers at all** unless all three conditions
hold:

| Variable | Required value | Why |
| --- | --- | --- |
| `HERDR_ENV` | `1` | Herdr's "this is a managed pane" marker |
| `HERDR_SOCKET_PATH` | path to the socket | the transport it reports over |
| `HERDR_PANE_ID` | e.g. `w1:p1` | which pane to report for |

Outside a Herdr pane none of these are set, so a normal terminal session pays
**zero** cost: no subprocesses, no socket, no event handlers.

> **Note:** unlike most extensions in this directory, `herdr.go` is **not** gated
> on the `extensions` array in `.kit/strictMode.json`. It adds no UI, and its
> own environment test is both necessary and sufficient. There is nothing to
> switch off.

## What Gets Reported

### Semantic state

Herdr understands exactly four states, and the extension only ever uses three:

| State | Meaning |
| --- | --- |
| `working` | the agent is actively doing something |
| `blocked` | the agent cannot continue without a decision from you |
| `idle` | the turn is over and you can type |

`unknown` is never reported, because Herdr treats it as "never seen" and the
pane would look brand new on every reload.

### Turn outcome

A turn that is interrupted or has failed is *not* the same as a finished one,
but Herdr has no state for that. So the outcome is published as a **display
label** instead (see [State vs. Label](#state-vs-label)):

| How the turn ended | Reported as |
| --- | --- |
| ran to completion | `ready` |
| you pressed ESC / cancelled | `interrupted` |
| provider or network error | `failed` |
| transient blip, still retrying | `retrying` |

So a pane that has been aborting for twenty minutes no longer looks identical
to a pane that is ready and waiting.

### Blocked

`blocked` is reported with a short message describing the wait, e.g.
`modal open` when a confirmation or selection popup is on screen, or
`waiting on user` when a turn is still live but not making progress.

`blocked` is the one state worth learning to recognise: it is what Herdr's
`agent wait --until blocked` and its notifications key on, so an agent that
needs you is distinguishable from one that is merely slow.

## State vs. Label

This is the single most important thing to understand, because it determines
what the extension reports and what Herdr acts on.

```
pane.report_agent      ->  state    ->  working | idle | blocked
                                      affects waits, notifications, rollups

pane.report_metadata   ->  label    ->  ready | interrupted | failed | retrying
                                      display only
```

Herdr's socket API draws this line explicitly: *"state carries semantic agent
state and affects waits, notifications and rollups. Report display-only values
separately through metadata."*

**Why outcomes cannot be states.** The server rejects anything outside the four
it knows, with:

```
invalid_request: unknown variant `error`, expected one of
`idle`, `working`, `blocked`, `unknown`
```

A rejected report is dropped entirely, so a clever `error` state would also
cost you the `working` / `blocked` information. The split keeps every semantic
value valid while still making the outcome visible.

The labels are published with `applies_to_source`, so they only ever decorate
states this integration itself owns and never touch another integration's.

## Troubleshooting

### Nothing appears in the Agents sidebar

Work through these in order:

1. **Is the pane managed by Herdr?** A plain `kit` started from a normal
   terminal has no Herdr environment. Check with:
   ```bash
   echo "$HERDR_ENV $HERDR_SOCKET_PATH $HERDR_PANE_ID"
   ```
   You should see `1` followed by a socket path and a pane id.

2. **Is Herdr's own detection taking the pane instead?** Herdr prefers its
   built-in agent detection. If it has claimed your pane, a second reporter
   competing for the same pane will see its state flip to `unknown`. Check
   `herdr agent explain`.

3. **Was the pane already claimed by another Kit session?** Only one source can
   hold a pane's lifecycle authority at a time. If two Kit processes are
   running in the same pane, they will fight. This is the single most common
   cause of a pane that will not settle.

### `herdr: pane.report_agent failed: ... broken pipe`

You may see this once, harmlessly, right after a Herdr server restart or live
handoff. Herdr closes the connection after answering each request, so an
in-flight report can hit a socket that has just been closed.

The extension retries automatically and does not surface a repeated failure.
If you are seeing it *persistently*, the socket path is stale:

```bash
ls -l "$HERDR_SOCKET_PATH"
```

### `herdr: pane.report_agent rejected by Herdr (stale_seq)`

Herdr keeps the last accepted `seq` per source per pane and discards anything
not strictly greater. The extension seeds its sequence from the wall clock
precisely so this cannot happen across restarts. Seeing it usually means a
second process on the same pane is reporting under the same source with an
older sequence.

### The pane shows `unknown`

`unknown` means no source currently owns the pane. In practice:

- the extension is not active (see step 1 above), or
- `herdr.go` did not load (check the startup log), or
- another process released the pane.

### Verifying by hand

The `herdr` CLI can confirm what the extension is doing without any extra
tooling:

```bash
herdr pane current              # which pane Herdr considers focused
herdr agent list                # panes currently tracked as agents
herdr agent explain             # why Herdr thinks a pane is (or is not) an agent
herdr pane layout --current     # pane ids, for cross-checking HERDR_PANE_ID
```

`herdr agent explain` is the most useful of these when something looks wrong:
it shows whether Herdr's own detection or a reported source is responsible for
the pane's state.

## Development

Run the tests:

```bash
cd ~/.config/kit/extensions
go test -run TestHerdr -v ./          # 46 tests
go test -race -run TestHerdr ./       # race detector
```

The suite drives the extension through the **production loader** and asserts on
the requests a **real fake Herdr server** receives over a real Unix socket, so
the wire format, connection reuse, redial and failure handling are exercised
rather than mocked.

### Modifying

Two things must be kept in sync by hand:

1. **The states are a closed set.** Herdr rejects anything outside
   `idle` / `working` / `blocked` / `unknown`. Add a new outcome by adding a
   *label*, never a state.
2. **The retry and the `seq` must move together.** A redial that reuses the old
   `seq` is dropped by Herdr, so the retry has to mint a fresh one.

`TestHerdr_OutcomesStillUseValidStates` and `TestHerdr_RetryUsesAHigherSeq`
exist to catch both mistakes.
