# lark-fs

Mirror everything reachable in Feishu/Lark onto disk as a **greppable, ID-addressed file tree**,
built on top of an already-logged-in [`lark-cli`](https://github.com/larksuite/cli).

```sh
m sync                      # incremental sync of everything
m sync --only messages docs # just these collections
m lark-status               # counts per collection, no network
lark-fs reindex             # rebuild users/ and media.yaml from messages on disk, no network
```

## Why a file tree

One entity per file, named by its Lark ID, so the query interface is just `fd` and `rg`:

```sh
rg 'ou_1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d' lark-data   # every mention of a person
fd om_x100b6d0e5c1a9f37e42b8d6c5a0f9e13               # locate one message
rg -l 'sandbox' lark-data/minutes                        # meetings that discussed sandboxes
```

Cross-entity references are always raw IDs, never paths — so grepping an ID finds
every place it appears, across messages, docs, minutes and wiki nodes alike.

Structured data is written as readable YAML (unquoted scalars, literal blocks for
multi-line text) rather than JSON, so message bodies stay greppable as plain lines
instead of being buried in `\n` escapes.

## Layout

```
<root>/
  .lark-fs/cursors.json               # per-collection incremental cursors
  chats/<chat_id>/
    meta.yaml  members.yaml  media.yaml    # image/file keys referenced by this chat
    messages/<YYYY-MM>/<message_id>.yaml
    threads/<thread_id>/<message_id>.yaml
  users/<open_id>/meta.yaml
  docs/<doc_token>/{meta.yaml,content.md,comments.yaml}
  minutes/<token>/{meta.yaml,transcript.txt,summary.md,chapters.yaml,todos.yaml}
  meetings/<meeting_id>/{meta.yaml,detail.yaml}    # detail.yaml links minute_token / note_id
  bases/<app_token>/tables/<table_id>/{meta.yaml,records.yaml}
  wiki/<space_id>/{meta.yaml,nodes.yaml}
```

## Incrementality

Lark rate-limits hard (`99991400`), so a full re-sync is never the plan:

- **messages** are walked forward in 12-hour slices, committing the cursor after each one.
  Two limits force this: `--page-all` caps at 40 pages (800 messages), so a wide window
  silently truncates; and the endpoint rate-limits hard enough that a long run *will* be
  cut short. An interrupted run resumes where it stopped rather than discarding its work.
- **docs / minutes / meetings** re-list metadata (cheap) but skip fetching bodies,
  transcripts and comments for entities already on disk.
- Requests are capped at 4 concurrent and retry with exponential backoff on 429.

The TUI shows one line per collection plus a live, bun-style list of the requests
currently on the wire. It stays on the normal screen buffer, so the final frame
remains in scrollback. Piping to a non-TTY falls back to one line per completed
collection, so CI still sees progress.

## Reindexing

`users/` and each chat's `media.yaml` are projections of the message files, so they can
be rebuilt locally when the extraction rules change — no API calls, no rate limit:

```sh
lark-fs reindex --root lark-data
```

## Setup

Needs `lark-cli` on PATH and authorized with:

```sh
lark-cli auth login --scope "im:chat:read im:chat.members:read"
```

Both are optional — messages come from the global search endpoint, so a sync still works
without them; they add chat listing (including quiet chats the message sweep never sees)
and the member roster that populates `users/`.

The store defaults to `~/lark-data` rather than `./lark-data`, so running from different
directories keeps one store instead of scattering copies. Override with `--root` or
`LARK_FS_ROOT`.
