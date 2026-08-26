# lark-fs

Mirrors Feishu/Lark onto disk as a greppable, ID-addressed file tree, driving the
`lark-cli` binary as a subprocess. See `README.md` for the layout and CLI.

## Working on this

- `uvx ruff check . && uvx ruff format .` and `uvx pyright` before committing. Both are
  configured strictly on purpose, and pyright reads the project venv via `pyproject.toml`.
- `uv run pytest tests` covers the failures that were silent. When adding one, revert the
  fix and watch it go red — a test that never failed proves nothing. Twice now a
  string-replace edit matched nothing and quietly "verified" an unchanged file.
- `m sync --only <collection>` to exercise one syncer against live data.
- Test against real Feishu data, never mocks — every bug found here so far was a
  wrong field name or an undocumented pagination cap that a mock would have hidden.

## lark-cli is inconsistent — verify, never guess

Each shortcut names its list field differently (`items` / `nodes` / `tables` /
`results` / `messages`), and a wrong guess does not error: it yields zero rows and
the sync reports success. Confirm the shape with a real call before writing a syncer:

    lark-cli <domain> <shortcut> --help
    lark-cli <domain> <shortcut> ... | head -c 500

Known traps, all hit in practice:

- `minutes +search` caps `--page-size` at 30; 50 is a hard 400.
- `im +messages-search` caps at 40 pages (800 messages) per query however the window is
  sliced — it cannot mirror a conversation. `im +chat-messages-list` has no such ceiling
  and reaches the first message in a chat (2020 vs 2026-07 on the same chat), so messages
  are walked per chat with a cursor each, never through search.
- Page-size ceilings differ per endpoint (50 for chat listing, 30 for `vc`/`minutes`
  search); `paginate` reads the cap out of the rejection and retries, so call sites do
  not carry the number.
- `base +record-list` defaults to a *markdown table*, is offset-based, and returns a
  column matrix (`fields` + `data` + `record_id_list`) rather than row objects.
- `base +table-list` wants `--base-token` (not `--app-token`) and keys tables by `id`.
- `docs +fetch` puts the body at `data.document.content`.
- `drive +search` returns a *ranked slice*, not the corpus: an empty query yielded 260 hits
  where the word 设计 alone yielded 400+. Coverage needs several probes unioned, plus the
  wiki node list (`obj_token`), which is enumerated exhaustively — that took docs 260 -> 3861.
- `im +chat-list` returns `chats`; members come from `+chat-members-list` as `users` + `bots`
  keyed by `member_id`, and need `im:chat.members:read` on top of `im:chat:read`.
- `wiki +node-list` returns one level; recurse via `has_child` + `--parent-node-token`.
- `minutes +detail --transcript` writes `<cwd>/minutes/<token>/transcript.txt` itself
  instead of returning it, so it must run with `cwd` set to the store root.
- Search endpoints return HTML-entity-escaped text with `<h>` hit markers; unescape it
  or the on-disk copy is not greppable. Use `_clean`, never bare `html.unescape` — the
  stdlib also decodes semicolon-less entities, so `&timestamp=` in a URL silently becomes
  a multiplication sign and `&notify=` a negation sign.
- Each entity has its own freshness signal, and they are not interchangeable:
  docs carry an authoritative `update_time`; messages expose one only through
  `+messages-mget` (never through search) and `updated` is a permanent flag, not a
  recency hint; recalled messages simply vanish from search, so the local copy is the
  only remaining record. `recheck_messages` replays known ids to catch both.

## YAML output must stay machine-readable

`yaml.py` is described upstream as display-only, but this project reads its own output
back (`lark-fs reindex`), so the rendered files must round-trip through a real parser.
Message bodies carry control characters and U+2028 line separators; unstripped, they
silently corrupt a literal block's indentation and the file stops parsing. `_sanitize`
handles this — verify any renderer change with `yaml.safe_load` over a real sync.

## Hot reload (hmr.py)

`uv run hmr.py <command>` reloads edited code into the running command -- `m sync` and
`m lark-watch` both go through it. Five things must hold, and
each fails silently on its own — verify with a marker that appears on *every* feed line,
not one that only shows in a transient state:

- Do not use the `hmr` CLI. It runs the entry synchronously and only starts watching
  afterwards, so an entry that blocks in `asyncio.run` never gets a watcher.
  `SyncReloaderAPI` watches on its own thread.
- The reloader re-executes its entry each time, so the entry cannot be hmr.py (infinite
  recursion) nor a package module (its relative imports break as `__main__`).
  `_reload_entry.py` exists solely for this.
- That entry must import the package. The reloader tracks what its entry touches; an
  empty one warns "has no dependencies and will never be auto-triggered" and never fires.
- Import the package only *after* the reloader exists — it patches `sys.meta_path`, and
  anything imported earlier is invisible to reloads.
- Reloading updates module namespaces in place, but a `run_with_tui` call already in
  flight built its closures from the old ones. The `post_reload` hook ends that cycle so
  the loop starts a new one. Ctrl-C raises the *same* exception as that abort, so the
  hook's flag is the only way to tell them apart — conflate them and the app cannot quit.

## Invariants

- Do not scan from a fixed start date. `_earliest` reads where the store's own data
  begins — a workspace whose history starts this year was otherwise spending 85% of its
  requests on empty months, every sync.

- One entity per file, named by its Lark ID. Cross-references are raw IDs, never paths.
- Structured data is YAML via `yaml.py` (literal blocks for multi-line text) so message
  bodies stay greppable as plain lines. Never write JSON for entity data.
- `Progress` rows are reactive: replace the whole row dict, never mutate in place, or
  subscribers are not notified.
- Progress that looks frozen is usually pacing, not rendering: a page of 20 messages is
  written in ~6ms and then the next request takes ~1.8s. `paginate(prefetch=True)` starts
  the next request first and spreads the current page across its flight time, so counters
  advance steadily. Measure repaints by counting PTY writes — matching whole lines misses
  partial redraws and makes a live UI look dead.
- A feed line should say what came back, not what was asked: `_summarise` pulls a count
  and a title out of the response, and the opaque request token is dropped once it
  returns (a time window is kept -- it *is* that request's identity). Verify feed changes
  against a real terminal with pyte; regex-splitting raw PTY bytes misreads partial
  redraws and will tell you a live UI is dead.
- The request feed is grouped per collection and shares a fixed line budget: only running
  collections get feed lines, so the layout never outgrows the screen. `cli.activity` owns
  that state, keyed by the `cli.current_group` ContextVar each syncer sets on entry.
  In-flight rows stay pinned below finished ones, so a row never jumps as it completes.
- The TUI repaints from a `@effect` subscribing to a `@derived` view, with
  `refresh_interval=0` — no polling. Create that effect only after `run_async` has
  started: `Application.invalidate()` is a no-op while the app is not running.
- A busy loop makes the process unkillable: the event loop never idles, so the SIGINT
  handler never runs and ctrl-c does nothing. Symptom is ~90% CPU with zero `lark-cli`
  children. Check that every `while` around a request actually advances its cursor.
- Fan out with `cli.spread`, never a bare `gather` over a whole backlog. The global
  semaphore hands out slots FIFO, so thousands of queued waiters from one collection sit
  ahead of every other row and freeze it completely — `docs` held all 8 slots while
  `messages` and `meetings` showed no in-flight requests at all. Bounding in-flight work
  costs no throughput; the semaphore was always the ceiling. Diagnose it with
  `pgrep -P <pid>`: if every child is the same subcommand, that is starvation, not a hang.
- A single chat can hold more messages than every other one combined (here: 181875, 16x
  the runner-up). Past `SLICE_AFTER` messages the rest of its range is walked as parallel
  `--start`/`--end` windows -- those bounds are exclusive at the top, so windows tile
  without gaps or overlap. Never store a window boundary as the cursor: the last one ends
  in the future and would skip everything sent between now and then.
- Count attempts, not successes. A syncer that returns early on `LarkError` without
  bumping leaves the row at 218/256 forever. Put the `p.bump` in a `finally`.
- One row, one denominator. A collection with a discovery pass and a fetch pass must
  reset `done` and re-set `total` when it moves on, or the fraction reads 3877/3877 while
  the requests visibly scrolling below it belong to a phase that is not being counted.
- Interruption is handled in `cli.run`, so every request is a cancellation point. Do not
  add per-loop stop checks in syncers — that approach already missed half of them.
- Two different ceilings exist and only one is survivable. 99991400 is per-second/minute
  throttling — back off and continue. 99991403 is the tenant's *monthly* API allowance,
  shared across every custom app, resetting on the 1st; retrying cannot clear it, so the
  run stops and says so. Never treat them the same.
- Rate limits are certain, not hypothetical. Anything that walks a long range must
  commit its cursor incrementally so an interrupted run resumes instead of restarting.
