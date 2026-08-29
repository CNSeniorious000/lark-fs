# lark-fs

Mirror everything reachable in Feishu/Lark onto disk as a **greppable, ID-addressed file tree**,
built on top of an already-logged-in [`lark-cli`](https://github.com/larksuite/cli).

## Install

Needs `lark-cli` on PATH; [uv](https://docs.astral.sh/uv/) brings its own Python 3.14, so
there is nothing else to set up. To run it once:

```sh
uvx git+https://github.com/CNSeniorious000/lark-fs sync
```

To keep it — this puts `lark-fs` on your PATH, and `uv tool upgrade lark-fs` pulls later
commits:

```sh
uv tool install git+https://github.com/CNSeniorious000/lark-fs
lark-fs sync
```

To hack on it — the syncers are where the Lark-specific knowledge lives, and you will
want to edit them:

```sh
git clone https://github.com/CNSeniorious000/lark-fs && cd lark-fs
uv run lark-fs sync
uv run hmr.py sync   # same thing, but edits to src/ land in the running TUI
```

## Authorize

`lark-cli` holds the credentials; this only drives it. A plain `lark-cli auth login` is
enough to start, and two extra scopes are worth asking for:

```sh
lark-cli auth login --scope "im:chat:read im:chat.members:read"
```

`im:chat:read` is not optional: messages are walked per chat, so a first sync with no
chats listed mirrors nothing. (The global message-search endpoint would not need it, but
it caps at 800 messages per query however the window is sliced, so it is not used.)
`im:chat.members:read` adds the member roster that populates `users/`, and
`contact:user:search` fills in the alias that tells two same-named colleagues apart —
without it the `profiles` pass reports that it could not run.

## Usage

```sh
lark-fs sync                      # incremental sync of everything -- also the default command
lark-fs sync --only messages docs # just these collections
lark-fs sync --only files         # fetch attachments for whatever is already indexed
lark-fs status                    # counts per collection, no network
lark-fs watch                     # poll for changes until interrupted
lark-fs reindex                   # rebuild users/, media.yaml and linked-doc stubs from disk, no network
```

The store defaults to `./lark-data`. Override with `--root` or `LARK_FS_ROOT` — the
latter is the way to keep one store while running from anywhere.

The message sweep is resumable: each chat's cursor is committed as it advances, so an
interrupted run picks up where it stopped rather than starting over. The other
collections resume by what is already on disk instead — see Incrementality.

## Why a file tree

One entity per file, named by its Lark ID, so the query interface is just `fd` and `rg`:

```sh
rg 'ou_1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d' lark-data   # every mention of a person
fd om_x100b6d0e5c1a9f37e42b8d6c5a0f9e13              # locate one message
rg -l 'sandbox' lark-data/minutes                    # meetings that discussed sandboxes
```

Cross-entity references are always raw IDs, never paths — so grepping an ID finds
every place it appears, across messages, docs, minutes and wiki nodes alike.

Structured data is written as readable YAML (unquoted scalars, literal blocks for
multi-line text) rather than JSON, so message bodies stay greppable as plain lines
instead of being buried in `\n` escapes.

## Layout

```
<root>/
  .lark-fs/cursors.json               # per-chat message cursors, sweep clocks, repair queues
  .lark-fs/config.toml                # which attachment kinds to mirror, and a size cap
  chats/<chat_id>/
    meta.yaml  members.yaml  media.yaml    # image/file keys referenced by this chat
    messages/<YYYY-MM>/<message_id>.yaml
    threads/<thread_id>/<message_id>.yaml
    files/<file_key>/<original name>       # the attachment itself, when its kind is enabled
  users/<open_id>/meta.yaml
  docs/<doc_token>/{meta.yaml,content.md,comments.yaml}
  minutes/<token>/{meta.yaml,transcript.txt,summary.md,chapters.yaml,todos.yaml}
  meetings/<meeting_id>.yaml                       # host, participants, minute_token / note_id
  notes/<note_id>.yaml                             # which documents a meeting note is made of
  bases/<app_token>/tables/<table_id>/{meta.yaml,records.yaml}
  wiki/<space_id>/{meta.yaml,nodes.yaml}
```

## Attachments

`media.yaml` always records every attachment key. Mirroring the *files* is opt-in per
kind, because each one costs a request against the tenant's monthly quota and a chat can
embed thousands of images. `.lark-fs/config.toml` is written with annotated defaults on
the first run:

```toml
[attachments]
kinds = ["text"]   # text, image, video, audio, doc, archive
max_mb = 10
extensions = []    # fetched whatever their kind, for anything the lists miss
```

`text` covers everything `rg` can read — source, config, logs, csv, json, subtitles, svg —
so an attached `.md` or `.jsonl` becomes searchable alongside the messages that carry it.
Lark never reports a size in advance, so `max_mb` is enforced after downloading; anything
over it is deleted and marked so it is not fetched again.

## Incrementality

Lark rate-limits hard (`99991400`), so a full re-sync is never the plan:

- **messages** are walked per chat, each with its own cursor committed as it advances, so
  an interrupted run resumes where it stopped. The global `+messages-search` endpoint is a
  trap here: it caps at 40 pages (800 messages) per query however the window is sliced.
  A chat still going after 2000 messages has the rest of its range split into 12-hour
  windows that page in parallel — one alert bot's chat holds 181875 messages, which is
  3600+ pages if walked as a single sequence.
- **docs / minutes / meetings** re-list metadata (cheap) but skip fetching bodies,
  transcripts and comments for entities already on disk. A doc's body is re-fetched when
  the server's `update_time` passes the copy on disk. Comments have no such signal —
  posting one does not touch `update_time` — so a document that has some is asked again
  once a day, and one that has none once a week.
- **discovery is not only search.** A ranked slice misses what nobody searched for, so
  every reference the mirror already holds is followed: documents linked in messages and in
  other documents' bodies (417 and 495 that no search returned), bitables named by the doc
  corpus (167 of 178), the minute a meeting's `detail.yaml` points at (191 of 710), and the
  documents a meeting note turns out to be made of. All of it is read off disk, so finding
  them costs nothing — only fetching does.
- **discovery passes have no incremental signal to offer at all** — a wiki node carries no
  update time and a search returns a ranked slice with no cursor — so they run on a clock
  instead: the wiki tree once a day, the doc search probes every six hours, chat rosters
  once a day, profiles once a week. Naming one explicitly (`--only wiki`) always sweeps it.
- **a meeting is one file**, because the two endpoints that describe it are two halves of
  one row, not two things: `vc +detail` is `meeting.get` plus the recording endpoint (its own
  `--dry-run` says so) and it emitted six of the twenty fields they return. The host, the
  status, the participant counts, the recording's duration and url, and the participant list
  — which `with_participants` adds to a request already being paid for — were all dropped.
- **minutes / meetings** are found by walking month by month, because `+search` caps the
  total a query can return. A window that comes back at that ceiling is halved down to a
  day, since a month is not always narrow enough. A plain sync walks only this month and
  the last — history does not grow backwards — and the walk over all of it is on the same
  six-hour clock, for an entry that lands further back than expected.
- **files** are skipped once their key's directory exists, including when it holds only
  the `.oversize` marker — which records the size that disqualified the file, so raising
  `max_mb` brings it back.
- **`watch` also re-verifies** already-synced messages, since edits and recalls leave no
  forward trace. It walks that window with a cursor rather than replaying it, so the tier
  costs a fixed 60 requests per run and covers everything about once a day.
- Requests are capped at 8 concurrent and retry with exponential backoff on 429. Each
  collection bounds its own in-flight work, or one of them would own the whole queue.

The TUI shows one line per collection plus a live, bun-style list of the requests
currently on the wire. It stays on the normal screen buffer, so the final frame
remains in scrollback. Piping to a non-TTY falls back to one line per completed
collection, so CI still sees progress.

## Reindexing

`users/` and each chat's `media.yaml` are projections of the message files, so they can
be refreshed locally when the extraction rules change — no API calls, no rate limit. Rows
are merged by key rather than replaced, so a reference to something no longer on disk
survives; delete the projection first if you want it rebuilt from nothing:

```sh
lark-fs reindex --root lark-data
```

## Developing

`uv run hmr.py <command>` runs any command under hot reload, so an edit to a syncer lands
in the TUI already on screen instead of costing you the sweep in flight. State is on
disk, so the restarted run resumes from the cursors.

```sh
uv run ruff check . && uv run ruff format .
uv run pytest          # regressions for the failures that were silent
pyright
```

`CLAUDE.md` is the field guide: `lark-cli`'s per-endpoint pagination caps, which
freshness signal each entity actually has, and the invariants that keep a partial sweep
from overwriting good data. Read it before touching `sync.py`.
