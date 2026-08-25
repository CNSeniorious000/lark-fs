# lark-fs

Mirrors Feishu/Lark onto disk as a greppable, ID-addressed file tree, driving the
`lark-cli` binary as a subprocess. See `README.md` for the layout and CLI.

## Working on this

- `uvx ruff check . && uvx ruff format .` before committing. Config is strict on purpose.
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
- `im +messages-search --page-all` silently stops at 40 pages (800 messages).
- `base +record-list` defaults to a *markdown table*, is offset-based, and returns a
  column matrix (`fields` + `data` + `record_id_list`) rather than row objects.
- `base +table-list` wants `--base-token` (not `--app-token`) and keys tables by `id`.
- `docs +fetch` puts the body at `data.document.content`.
- `im +chat-list` returns `chats`; members come from `+chat-members-list` as `users` + `bots`
  keyed by `member_id`, and need `im:chat.members:read` on top of `im:chat:read`.
- `wiki +node-list` returns one level; recurse via `has_child` + `--parent-node-token`.
- `minutes +detail --transcript` writes `<cwd>/minutes/<token>/transcript.txt` itself
  instead of returning it, so it must run with `cwd` set to the store root.
- Search endpoints return HTML-entity-escaped text with `<h>` hit markers; unescape it
  or the on-disk copy is not greppable.

## YAML output must stay machine-readable

`yaml.py` is described upstream as display-only, but this project reads its own output
back (`lark-fs reindex`), so the rendered files must round-trip through a real parser.
Message bodies carry control characters and U+2028 line separators; unstripped, they
silently corrupt a literal block's indentation and the file stops parsing. `_sanitize`
handles this — verify any renderer change with `yaml.safe_load` over a real sync.

## Invariants

- One entity per file, named by its Lark ID. Cross-references are raw IDs, never paths.
- Structured data is YAML via `yaml.py` (literal blocks for multi-line text) so message
  bodies stay greppable as plain lines. Never write JSON for entity data.
- `Progress` rows are reactive: replace the whole row dict, never mutate in place, or
  subscribers are not notified.
- Rate limits are certain, not hypothetical. Anything that walks a long range must
  commit its cursor incrementally so an interrupted run resumes instead of restarting.
