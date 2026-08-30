"""Entity syncers. Each is an async function that pulls one collection into the store.

Incrementality: every collection stores a cursor (usually the newest timestamp
seen last run) in .lark-fs/cursors.json, and only asks Lark for what came after.
Search-backed collections (docs/minutes/meetings) have no server-side cursor, so
they re-list metadata (cheap) but skip fetching bodies for entities already on disk.
"""

from asyncio import create_task, gather
from bisect import bisect_right
from collections.abc import Awaitable, Callable, MutableMapping
from contextlib import aclosing
from datetime import UTC, datetime, timedelta, timezone
from itertools import islice
from json import dumps
from pathlib import Path
from re import MULTILINE, compile
from typing import Any

from reactivity import reactive

from . import cli
from .attachments import sync_attachments
from .cli import Aborted, SyncAbortedError
from .store import Store

TZ = "+08:00"


FIRST_MONTH = "2023-01-01"  # only used the first time, before the store can answer


def _earliest(store: Store, collection: str, since: str, pattern: str = "*/*.yaml") -> datetime:
    """Where the month walk should start.

    Scanning from a fixed year re-queries every empty month before the account had any
    data -- for a workspace that started this year that is most of the requests. Once
    anything is on disk it says where history actually begins; back up one month so an
    entry that lands late still gets picked up.

    `pattern` because the two collections that walk months are not the same shape: a minute
    is a directory of artifacts, a meeting is one file.
    """
    if since:
        return datetime.fromisoformat(since).replace(tzinfo=UTC)
    months = [m[1].replace(".", "-") for f in (store.root / collection).glob(pattern) if (m := RE_START.search(f.read_text()))]
    if not months:
        return datetime.fromisoformat(FIRST_MONTH).replace(tzinfo=UTC)
    return (datetime.fromisoformat(f"{min(months)}-01").replace(tzinfo=UTC) - timedelta(days=1)).replace(day=1)


def _recent(now: datetime) -> datetime:
    """The start of last month.

    A month already walked cannot gain entries -- history does not grow backwards -- so
    re-listing all of it on every sync spends requests to learn nothing. Measured: 102 of
    one sync's 435 requests were the minutes walk and 57 were the meetings walk, 36% of the
    run. Only the current month and the one before it can still change, and the full walk
    stays on a clock for the case a late entry lands further back.
    """
    return (now.replace(day=1) - timedelta(days=1)).replace(day=1)


def _months_between(start: datetime, end: datetime) -> int:
    return (end.year - start.year) * 12 + end.month - start.month + 1


# `need_transcript` too: the transcript comes back inside this response, and `+detail` was
# only writing it to a file for us. Everything here is one request.
ARTIFACTS = '{"need_summary":true,"need_todo":true,"need_chapter":true,"need_keyword":true,"need_transcript":true}'
MINUTE_CARD_KEYS = ("display_info", "meta_data")  # what only `minutes +search` returns
RE_MINUTE_TOKEN = compile(r"^minute_token: '?([A-Za-z0-9]+)'?$", MULTILINE)
RE_BITABLE = compile(r"^(?:doc_types: BITABLE|obj_type: bitable)$", MULTILINE)  # a doc meta names its own type; a title that merely says "bitable" is not one
RE_TENANT = compile(r"https://[a-z0-9-]+\.(?:feishu\.cn|larksuite\.com)")


def _note_tenant(text: str):
    """Remember the tenant's own host the first time Lark hands one over.

    Nothing in the API reports it -- `auth status` returns an app id and an open id, not a
    host -- yet every doc, minute and base link needs it. The URLs in search results carry
    it, so the mirror learns it from its own data instead of asking anyone to configure it.
    """
    if not cli.TENANT and (m := RE_TENANT.search(text or "")):
        cli.TENANT = m[0]


def _learn_tenant(store: Store):
    """Recover it from disk, so a later run has links before docs are re-listed."""
    for f in islice((store.root / "docs").glob("*/meta.yaml"), 40):
        _note_tenant(f.read_text())
        if cli.TENANT:
            return


# A chat this dense is walked in parallel windows instead of one page sequence. The width
# is a load-balancing knob, not a correctness one: every window pages to its own end, so
# too wide leaves one slot working alone and too narrow spends requests on empty air.
SLICE_AFTER = 2000
SLICE_HOURS = 12
# Wiki levels are the exception to the global concurrency: `+node-list --page-all` fans out
# internally, so eight at once is ~80 requests in a burst and Lark throttles it. Measured
# over a full tree: at 8 it lost 315 nodes to 35 rate limits; at 3 it walked all 2366 with
# none, for 108s instead of 43s. A tree walked once a day can afford the difference.
WIKI_WIDTH = 3
# How long a pass that has no incremental signal of its own may coast before a plain `sync`
# runs it again. Neither the wiki walk nor the doc search has one to offer, so this is the
# only lever on what they cost -- and the wiki comment above already says a tree walked once
# a day can afford its width. Rosters and profiles are here for a different reason: people
# join, leave and are renamed, and "the file exists" says nothing about any of that.
WIKI_HOURS = 24.0
SEARCH_HOURS = 6.0
ROSTER_HOURS = 24.0  # one request per chat, 200 on this store
PROFILE_HOURS = 168.0  # one per 19 users, and a name or a department moves far more slowly than a roster
MEETING_SETTLE_DAYS = 7
# Both search endpoints reject anything above 30 outright, and `paginate`'s default is 50:
# every window paid for a rejection before retrying at the cap the error named. `drive
# +search` is the other shape -- it clamps silently to 20 and answers, so it costs nothing.
SEARCH_PAGE = 30
COMMENT_HOURS = 24.0  # for a document that has comments; the empty ones are 87% of the corpus and wait a week
COMMENT_EMPTY_HOURS = 168.0
BASE_HOURS = 24.0  # one `+table-list` per base, 178 of them
NOACCESS_HOURS = 168.0  # how long anything nobody shared with us is left alone before asking again
META_BATCH = 200  # `drive metas batch_query` refuses more request_docs than this
RECHECK_FRESH_SECONDS = (
    3600.0  # how recently written counts as "just synced", for the half of the recheck that is not a sweep  # a minute appears only once the recording is processed, well after the meeting ends
)
STAMP = "%Y-%m-%d %H:%M"  # what the API both returns in create_time and accepts in --start/--end
TENANT_TZ = timezone(timedelta(hours=8))  # the same offset as TZ, as a clock rather than a suffix


def _windows(start: str, hours: int) -> list[tuple[str, str]]:
    """Split [start, now] into fixed-width windows that can be paged independently.

    `create_time` carries no offset, so "now" has to be read on the tenant's wall clock --
    against a UTC one the last windows would land eight hours short and drop that history.
    """
    t = datetime.strptime(start[:16], STAMP).replace(tzinfo=TENANT_TZ)
    now, out = datetime.now(TENANT_TZ), []
    while t < now:
        nxt = t + timedelta(hours=hours)
        out.append((t.strftime(STAMP), nxt.strftime(STAMP)))
        t = nxt
    return out


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + TZ


type Row = dict[str, Any]


class Progress:
    """Sync state as a reactive store; readers re-run automatically when a row changes.

    Rows are replaced wholesale rather than mutated in place -- the reactive mapping
    tracks per-key identity, so an in-place dict update would not notify subscribers.
    """

    def __init__(self):
        self.rows: MutableMapping[str, Row] = reactive({})

    def _row(self, name: str) -> Row:
        return self.rows.get(name) or {"name": name, "state": "pending", "done": 0, "total": None, "note": "", "last": ""}

    def set(self, name: str, **fields):
        cli.current_group.set(name)  # one source of truth: the row you report under owns your requests
        self.rows[name] = {**self._row(name), **fields}

    def bump(self, name: str, n: int = 1, last: str = ""):
        """`last` is what was just written -- a title or id -- shown so the run is legible."""
        row = self._row(name)
        self.rows[name] = {**row, "state": "running", "done": row["done"] + n, "last": last or row["last"]}


async def sync_messages(store: Store, p: Progress, *, chat_ids: set[str] | None = None):
    """Messages, walked per chat with a cursor each.

    The obvious route -- the global `+messages-search` endpoint -- is a trap: it caps at
    40 pages (800 messages) per query no matter how the window is sliced, so a sweep over
    it silently mirrors only the most recent slice of each conversation. `+chat-messages-list`
    has no such ceiling and reaches back to the first message in a chat; listing chats and
    walking each one is both complete and cheaper to resume, because a per-chat cursor
    means an interrupted run only re-reads the chat it stopped in.
    """
    known = chat_ids or await _list_chats(store)
    cursors: dict[str, str] = store.cursors.setdefault("chats", {})
    p.set("messages", state="running", total=len(known), done=0, note=f"{len(known)} chats")
    media: list[dict] = []
    links: list[tuple[str, str]] = []
    users: set[str] = set()
    total = 0

    async def drain(chat_id: str, start: str, end: str = "", limit: int = 0) -> tuple[str, bool]:
        """Walk one time window of a chat. Returns the newest create_time seen, and whether `limit` cut it short."""
        nonlocal total
        # Reactions come back with the page, for one extra request per 20 messages, and 11%
        # of messages carry one -- in a work chat a 👌 from the right person is the answer, and
        # nothing else in the mirror records it. Only the *walk* asks: it meets a message once,
        # at ~2300 a day. The recheck tier below re-reads 3000 already-known messages every
        # half hour, so asking there would quadruple the daemon's steady cost forever.
        argv = ["im", "+chat-messages-list", "--chat-id", chat_id, "--order", "asc", *(["--start", start] if start else []), *(["--end", end] if end else [])]
        newest, seen = "", 0
        # this loop returns early once `limit` is reached, and the generator holds a
        # prefetched page in flight -- without aclosing that request is paid for, thrown
        # away, and its cleanup deferred to garbage collection
        async with aclosing(cli.paginate(*argv, key="messages", prefetch=True)) as pages:
            async for msg in pages:
                if not (mid := msg.get("message_id")):
                    continue
                created = msg.get("create_time", "")
                if thread := msg.get("thread_id"):
                    written = _write_thread(store, chat_id, thread, msg)
                else:
                    store.write_yaml(f"chats/{chat_id}/messages/{created[:7] or 'unknown'}/{mid}.yaml", _clean(msg))
                    written = [msg]
                for one_msg in written:  # a thread arrives as one item but is many messages
                    media.extend(_index_media(one_msg))
                    links.extend(_doc_links(str(one_msg.get("content") or "")))
                    _record_sender(store, one_msg, users)
                total += len(written)
                newest = max(newest, created)
                if total % 20 == 0:
                    sender = (msg.get("sender") or {}).get("name") or ""
                    p.set("messages", last=f"{sender}: {cli.oneline(msg.get('content'), 40)}", note=f"{total} messages")
                if limit and (seen := seen + 1) >= limit:
                    return newest, True
        return newest, False

    async def one(chat_id: str):
        since = cursors.get(chat_id) or ""
        try:
            newest, dense = await drain(chat_id, since, limit=SLICE_AFTER)
        except cli.LarkError:
            p.bump("messages")  # one unreadable chat must not sink the sweep; its cursor stays put
            return
        if dense:
            # An alert bot's chat runs to six figures. Paged in one sequence it walks
            # thousands of pages on a single slot and holds the whole sweep open long
            # after every other chat is done, so the rest of it is split into windows
            # that page independently.
            windows = _windows(newest, SLICE_HOURS)
            gap, high, filled = "", newest, 0

            async def window(bounds: tuple[str, str]):
                nonlocal gap, high, filled
                try:
                    seen, _ = await drain(chat_id, *bounds)
                    high = max(high, seen)
                except cli.LarkError:
                    gap = min(gap or bounds[0], bounds[0])
                filled += 1
                p.set("messages", note=f"{total} messages, {filled}/{len(windows)} windows of one busy chat")

            await cli.spread(window, windows)
            # The cursor tracks the newest message actually read, never a window boundary:
            # the last window ends in the future, and storing that would skip every message
            # sent between now and then. It also may not pass a window that failed, or that
            # slice of history is gone for good.
            newest = min(gap, high) if gap else high
        if newest and newest != since:
            # The media index has to be durable before the cursor is. These rows only exist
            # in memory until the sweep ends, and the cursor is written per chat -- so a
            # ctrl-c, an exhausted quota, or any other collection raising in between leaves
            # a cursor past messages whose attachments were never recorded. Nothing looks
            # at those messages again; only a full `reindex` finds them.
            _flush_media(store, [r for r in media if r.get("chat_id") == chat_id])
            # The cursor is the last create_time read, unadvanced. It has to be: `create_time`
            # has minute resolution and `--start` includes that minute, so moving the cursor
            # past it would skip every message that arrives later in the same minute -- and
            # those are common, not an edge case (861 same-minute groups in 4000 files
            # sampled). The cost of not advancing is re-reading one minute per chat per run,
            # which `Store.write` turns into no writes at all. Measured: asking with a
            # message's own create_time returns that message.
            cursors[chat_id] = newest
            store.save_cursors()
        p.bump("messages")

    await cli.spread(one, sorted(known))

    async def repair(item: tuple[str, str]):
        # a truncated thread has at least 50 replies, so an empty answer is a failed fetch:
        # leave it on the list rather than recording it as repaired
        if replies := await repair_thread(store, *item):
            for r in replies:
                media.extend(_index_media(r))
                links.extend(_doc_links(str(r.get("content") or "")))
                _record_sender(store, r, users)
            cut.pop(item[0], None)
        p.set("messages", note=f"{total} messages, {len(cut)} truncated threads left")

    # A chat listing inlines at most 50 replies per thread; only this list remembers which
    # threads owe more, because the sweep will not pass them again -- the cursor moved on.
    if cut := store.cursors.get("threads_incomplete") or {}:
        await cli.spread(repair, sorted(cut.items()))
        store.save_cursors()

    async def rewrite(chunk: list[tuple[str, str]]):
        try:
            fetched = await repair_unreadable(store, chunk)
        except cli.LarkError:
            return  # the request failed, not the messages: they stay queued for the next run
        for msg in fetched:
            media.extend(_index_media(msg))
            links.extend(_doc_links(str(msg.get("content") or "")))
            _record_sender(store, msg, users)
        for mid, _ in chunk:
            broken.pop(mid, None)  # asked for and not returned: recalled, and not coming back either
        p.set("messages", note=f"{total} messages, {len(broken)} unreadable files left")

    # The file is on disk but unparseable, so only the API still holds the real content.
    # `reindex` is what finds them -- a scan of every message is far too slow to run here.
    if broken := store.cursors.get("messages_unreadable") or {}:
        items = sorted(broken.items())
        await cli.spread(rewrite, [items[i : i + 50] for i in range(0, len(items), 50)])
        store.save_cursors()

    _flush_media(store, media)
    # once for the whole run: the stubs are global, unlike the per-chat media index, and the
    # alias table this reads is 2371 rows
    _flush_doc_links(store, links)
    p.set("messages", state="done", note=f"{total} messages across {len(known)} chats")
    return known


async def _list_chats(store: Store, group: str = "messages") -> set[str]:
    """Every chat the user belongs to, plus whatever is already mirrored.

    It runs as a task of its own, so it inherits whichever group happened to be current
    when it was spawned -- the daemon's, between sweeps -- and files its pages under a row
    that issues no other request, where nothing ever displaces them. Claim the row that
    wants the answer instead.

    A listing that fails on its third page still returns two pages of chats, and taking
    that as the answer silently drops every chat it had not reached yet -- for that whole
    run, message sweep included. The disk is not a fallback for that case, it is the floor:
    a chat that was mirrored once still exists whether or not this listing reached it.
    """
    cli.current_group.set(group)
    found: set[str] = set()
    try:
        async for chat in cli.paginate("im", "+chat-list", "--types", "p2p,group", key="chats"):
            if cid := chat.get("chat_id"):
                found.add(cid)
                store.write_yaml(f"chats/{cid}/meta.yaml", _clean(chat))
    except cli.LarkError:
        pass
    return found | {d.name for d in (store.root / "chats").glob("oc_*")}


def _reason(e: cli.LarkError) -> str:
    return e.payload.get("error", {}).get("message", "failed") if isinstance(e.payload, dict) else "failed"


# media keys are embedded in the message body, not a separate field:
#   `[Image: img_v3_...]`  `![Image](img_v3_...)`  `<file key="file_v3_..." name="..."/>`
RE_START = compile(r"(?:start_time|开始时间):\s*'?(20\d{2}[-.]\d{2})")  # anchor on the label; bare digits also match durations and ids
RE_MEDIA = compile(r'(?:\[(?:Image|Media|File|Video|Audio):\s*([a-z]+_v\d+_[\w-]+)\]|!\[[^\]]*\]\(([a-z]+_v\d+_[\w-]+)\)|<\w+\s+key="([^"]+)"(?:\s+name="([^"]*)")?)')


def _flush_media(store: Store, rows: list[dict]):
    """Merge new media refs into each chat's index, keyed by media key so re-runs don't duplicate."""
    by_chat: dict[str, list[dict]] = {}
    for row in rows:
        by_chat.setdefault(row["chat_id"], []).append(row)
    for chat_id, new in by_chat.items():
        rel = f"chats/{chat_id}/media.yaml"
        merged = {r["key"]: r for r in store.read_yaml_rows(rel)} | {r["key"]: r for r in new}
        store.write_yaml(rel, sorted(merged.values(), key=lambda r: r["create_time"]))


# What a Lark URL calls a document, and what `+list-comments` calls the same thing. `/file/`
# is deliberately absent: it is a Drive file, which exports no markdown -- and document
# bodies link 13203 of them, mostly their own embedded attachments, against 495 real
# documents. A stub each would be a fetch and a comments call apiece for a title.
LINK_TYPES = {"docx": "docx", "docs": "doc", "sheets": "sheet", "base": "bitable", "slides": "slides", "wiki": "wiki"}
# A token is 27 characters, or 26 for an old-style `wikcn` node -- 3628 of the 3633 on
# disk. A looser bound reads whatever a message glued onto the end of the URL as part of
# the token, which mirrors a document that does not exist *instead of* the one that does:
# five such directories, one of them hiding a real document nothing else had found. The
# match is greedy, so it takes the 27 that are the token and leaves the rest.
RE_DOC_LINK = compile(r"(?:feishu\.cn|larksuite\.com)/(" + "|".join(LINK_TYPES) + r")/([A-Za-z0-9]{26,27})")


def _doc_links(text: str) -> list[tuple[str, str]]:
    """Documents named by a link in this text, as (token, type).

    A search returns a ranked slice and the wiki walk only covers the wiki, so between them
    they miss what people actually pass around and what documents point at: 417 of the 900
    linked in these chats had never been mirrored, and another 495 are linked from document
    bodies. Reading the link costs nothing -- the text is already in hand -- and its path
    segment is the type, which is the one thing `+list-comments` cannot be asked without.

    A `/wiki/` link is a node token, not the document's own: measured on five of them, every
    one answers 1069307 as `docx` and answers as `wiki`.
    """
    return [(token, LINK_TYPES[kind]) for kind, token in RE_DOC_LINK.findall(text)]


def _flush_doc_links(store: Store, links: list[tuple[str, str]]):
    """Write a stub for each linked document the mirror does not have.

    Only the token and its type; the title, body and comments belong to the doc pass, which
    picks this up the same way it picks up anything else already on disk. A wiki link the
    node lists resolve is the same document under another name, and mirroring it under both
    would store it twice -- so the alias table is read once here rather than per chat.
    """
    alias = _wiki_aliases(store) if any(kind == "wiki" for _, kind in links) else {}
    for token, kind in links:
        if store.exists(f"docs/{token}/meta.yaml") or (token in alias and store.exists(f"docs/{alias[token]}/meta.yaml")):
            continue
        store.write_yaml(f"docs/{token}/meta.yaml", {"token": token, "doc_types": kind.upper(), **({"comment_type": "wiki"} if kind == "wiki" else {})})


def _thread_meta(root: dict, replies: list[dict]) -> dict:
    """The thread's own identity, so the directory is not merely a bag of messages.

    The root keeps its own `<message_id>.yaml` like every other message: thread roots are
    never also written under `messages/` (measured: 0 of 400), so naming the root file
    meta.yaml would put 16717 real messages out of reach of `fd <message_id>`.
    """
    return {
        "thread_id": root.get("thread_id", ""),
        "chat_id": root.get("chat_id", ""),
        "root_message_id": root.get("message_id", ""),
        "replies": len(replies),
        "has_more": bool(root.get("thread_has_more")),
        "create_time": root.get("create_time", ""),
        "last_reply": max((r.get("create_time") or "" for r in replies), default=root.get("create_time", "")),
        "link": root.get("message_app_link", ""),
    }


def _write_thread(store: Store, chat_id: str, thread: str, msg: dict) -> list[dict]:
    """Store a thread as one file per message. Returns every message, for the projections.

    Lark hands a thread over as its root with every reply nested inside it. Written that
    way a reply has no file of its own, so `fd <message_id>` cannot find it -- and
    `_index_media` and `_record_sender` read one message's *own* fields, so neither ever
    sees one. Measured on a full mirror before this: 49308 replies, carrying 3254
    attachment keys and 912 people, none of them in any index.
    """
    replies = msg.get("thread_replies") or []
    root = {k: v for k, v in msg.items() if k != "thread_replies"}
    at = f"chats/{chat_id}/threads/{thread}"
    # A listing inlines at most 50 replies, and a repair may already have written more, so
    # the inline view is a floor and not the truth. Reporting it as the truth walked a
    # thread's count backwards -- one on this store said `replies: 0` beside five reply
    # files -- and reset `has_more`, which re-queued a repair that had already finished.
    on_disk = {p.stem for p in (store.root / at).glob("*.yaml")} - {"meta", root.get("message_id", "")}
    meta = _thread_meta(root, replies)
    known = on_disk | {r.get("message_id", "") for r in replies}
    store.write_yaml(f"{at}/meta.yaml", {**meta, "replies": max(len(replies), len(known)), "last_reply": max(meta["last_reply"], store.read_yaml(f"{at}/meta.yaml").get("last_reply") or "")})
    for m in (root, *replies):
        store.write_yaml(f"{at}/{m['message_id']}.yaml", _clean(m))
    if root.get("thread_has_more"):
        # `+chat-messages-list` inlines at most 50 replies and says so; the rest exist only
        # through the thread's own endpoint, and this sweep will not pass here again --
        # the cursor moves past. Remembering it is the only thing that keeps them reachable.
        store.cursors.setdefault("threads_incomplete", {})[thread] = chat_id
    return [root, *replies]


def migrate_threads(store: Store) -> int:
    """Split threads written as one nested blob into one file per message.

    Runs at the head of every sync, because a store synced by an older version has those
    replies on disk and nothing else will ever look at them again -- the sweep only
    revisits a chat from its cursor forward.

    Every thread directory ends up with a meta.yaml, including one whose thread has no
    replies at all. That file is what this scan short-circuits on, and a thread that never
    earned one is re-read on every single sync: measured on a real mirror, the 4677
    unreplied threads were 3.6s of the 3.7s an already-migrated store was paying.
    """
    split = 0
    for at in sorted(store.root.glob("chats/*/threads/*")):
        if (at / "meta.yaml").exists():
            continue
        messages = [store.read_yaml(str(p.relative_to(store.root))) for p in sorted(at.glob("*.yaml"))]
        nested = [m for m in messages if m.get("thread_replies")]
        if root := (nested or messages or [{}])[0]:
            _write_thread(store, at.parents[1].name, at.name, root)
            split += bool(nested)
    return split


def migrate_minutes(store: Store) -> int:
    """Fold `minutes/<t>/{detail,chapters,todos}.yaml` into that minute's `meta.yaml`.

    Four YAML files for two endpoints, and the key sets never overlapped: measured over the
    553 that have both, `meta` was always {display_info, meta_data, token} and `detail`
    always {minute_token, title, note_id}. Chapters and todos arrive inside the same
    `artifacts` response as the summary, so a file boundary there said nothing.

    Nothing is fetched. The record keeps what was on disk under the names `+detail` gave it,
    and the next sweep replaces those with the raw endpoint's own -- which is also where the
    owner, duration, cover, url and keywords finally land.
    """
    folded = 0
    for at in sorted(store.root.glob("minutes/*")):
        parts = {n: at / f"{n}.yaml" for n in ("detail", "chapters", "todos")}
        if not at.is_dir() or not any(f.exists() for f in parts.values()):
            continue
        row = store.read_yaml(f"minutes/{at.name}/meta.yaml")
        for name, f in parts.items():
            if f.exists():
                loaded = store.read_yaml(str(f.relative_to(store.root)))
                row = {**row, **(loaded if name == "detail" else {name: loaded})}
                f.unlink()
        store.write_yaml(f"minutes/{at.name}/meta.yaml", row)
        folded += 1
    return folded


def migrate_meetings(store: Store) -> int:
    """Fold `meetings/<id>/{meta,detail}.yaml` into one `meetings/<id>.yaml`.

    The two halves were two endpoints, not two things: measured over all 849, their
    top-level key sets never once overlapped and `meta.id == detail.meeting_id == dirname`
    on every one. The only fact `meta` held that `detail` did not was the organiser's
    display name, rendered into a card -- and `vc meeting get` returns the organiser as an
    open_id, which is the form the rest of this store links by.

    Nothing is fetched here: the merged record keeps what was already on disk, and having
    no `participants` key is what marks it due, so the richer shape arrives with the next
    meetings sweep. Written before the directory is removed, so a run killed in between
    leaves a store that reads correctly and finishes migrating next time.
    """
    folded = 0
    for at in sorted(store.root.glob("meetings/*")):
        if not at.is_dir():
            continue
        row = {**store.read_yaml(f"meetings/{at.name}/meta.yaml"), **store.read_yaml(f"meetings/{at.name}/detail.yaml")}
        store.write_yaml(f"meetings/{at.name}.yaml", row)
        for f in at.iterdir():
            f.unlink()
        at.rmdir()
        folded += 1
    return folded


async def repair_thread(store: Store, thread: str, chat: str) -> list[dict]:
    """Fetch every reply of a thread that was cut off at the inline cap, and rewrite it.

    `+chat-messages-list` inlines at most 50 replies per thread and sets
    `thread_has_more`; the rest exist only through the thread's own endpoint, which does
    paginate. Measured on a real truncated thread: 50 on disk, 52 from here.

    Returns the replies, so the caller can index what they carry. Empty on failure, which
    leaves the thread on the repair list for the next run.
    """
    at = f"chats/{chat}/threads/{thread}"
    try:
        replies = [m async for m in cli.paginate("im", "+threads-messages-list", "--thread", thread, key="messages") if m.get("message_id")]
    except cli.LarkError:
        return []
    for r in replies:
        store.write_yaml(f"{at}/{r['message_id']}.yaml", _clean(r))
    if not replies:
        # An empty answer is the failure the caller already reads it as. Writing meta here
        # anyway set `replies: 0, has_more: False` over a count that was right, left the
        # files it described orphaned, and kept the thread queued -- refetched every run,
        # with the record of what it holds now saying it holds nothing.
        return []
    meta = store.read_yaml(f"{at}/meta.yaml")
    store.write_yaml(f"{at}/meta.yaml", {**meta, "replies": len(replies), "has_more": False, "last_reply": max((r.get("create_time") or "" for r in replies), default=meta.get("last_reply", ""))})
    return replies


async def repair_unreadable(store: Store, chunk: list[tuple[str, str]]) -> list[dict]:
    """Re-fetch messages whose file on disk cannot be parsed, and write them again.

    A file written by a serializer that could not round-trip its content is on disk and
    says nothing readable, so the API is the only place the message still exists. Takes
    (message_id, relative path) pairs, at most 50 -- what `+messages-mget` accepts.

    Returns what came back. A message the server no longer has simply does not come back,
    and no amount of retrying will change that -- but a request that *failed* says nothing
    about the messages in it, so the error is raised rather than reported as an empty
    answer. Told apart only by the caller, which is what clears the repair list.
    """
    data = await cli.run("im", "+messages-mget", "--message-ids", ",".join(mid for mid, _ in chunk))  # a repaired message has to be as complete as a freshly walked one
    returned = {m["message_id"]: m for m in (data or {}).get("messages") or []}
    written = []
    for mid, rel in chunk:
        if msg := returned.get(mid):
            store.write_yaml(rel, _clean(msg))
            written.append(msg)
    return written


def _index_media(msg: dict) -> list[dict]:
    """Collect media references as keys and a link to the message holding them.

    Bytes are never downloaded here. Lark gives an attachment no address of its own -- a
    key is only usable through `+messages-resources-download` -- so the closest thing to
    the URL the design asked for is the applink of the message it arrived in, which opens
    it in Feishu. It costs nothing: the message already carries it.
    """
    rows = [
        {
            "key": img or md or key,
            "name": name or "",
            "msg_type": msg.get("msg_type", ""),
            "message_id": msg.get("message_id"),
            "chat_id": msg.get("chat_id"),
            "create_time": msg.get("create_time", ""),
            "link": msg.get("message_app_link", ""),
        }
        # a message body is not always a string: one real message is a 48-digit number, and
        # `cli.oneline` already coerces for the same reason
        for img, md, key, name in RE_MEDIA.findall(str(msg.get("content") or ""))
    ]
    return rows


async def sync_chat_meta(store: Store, p: Progress, chat_ids: set[str]):
    """Chat rosters. Metadata already landed when messages listed the chats.

    Membership is the part only this pass can get, and it is the expensive part, so a chat
    whose roster is on disk is skipped -- but only until the clock comes due. People join,
    leave and change their names, and "the file exists" is not a statement about any of
    that: it froze the first roster ever fetched as the permanent one. Refreshing costs one
    request per chat, 200 on this store, which is why it is a day rather than a run.
    """
    p.set("chats", state="running")
    known = chat_ids or {d.name for d in (store.root / "chats").glob("oc_*")}
    stale = not swept_recently(store, "rosters", ROSTER_HOURS)
    todo = [c for c in known if stale or not store.exists(f"chats/{c}/members.yaml")]
    p.set("chats", total=len(todo), note=f"{len(known)} chats")
    if not todo:
        p.set("chats", state="done", note=f"{len(known)} chats, rosters up to date")
        return

    reached = 0

    async def one(chat_id: str):
        nonlocal reached
        try:
            # `--page-all` is not all of them: lark-cli stops after ten pages unless told
            # otherwise, and at the max page size that is a thousand. Four chats sat at
            # exactly 1000 members with `has_more` still set -- the largest really has 3296.
            members = await cli.run("im", "+chat-members-list", "--chat-id", chat_id, "--page-all", "--page-limit", "0")
        except cli.LarkError:
            p.bump("chats")  # p2p chats and ones we lack scope for simply have no roster
            return
        reached += 1
        store.write_yaml(f"chats/{chat_id}/members.yaml", _clean(members))
        # bots and users share one directory, and only 67 of 81 bots seen carry app_id -- without
        # this flag the other 14 are indistinguishable from people once on disk
        people = ((members or {}).get("users") or []) + [{**b, "is_bot": True} for b in ((members or {}).get("bots") or [])]
        for u in people:
            if oid := u.get("member_id") or u.get("open_id"):
                store.write_yaml(f"users/{oid}/meta.yaml", _clean({"open_id": oid, **u}))
        p.bump("chats", last=f"{len(people)} members")

    await cli.spread(one, todo)
    if stale and reached:
        record_sweep(store, "rosters")
    p.set("chats", state="done", note=f"{len(known)} chats")


# Everything a roster carries about a person beyond their id. `localized_name` is the one
# that matters: it holds the parenthesised alias that tells two same-named colleagues apart.
# The last two say things no other endpoint does: whether this person belongs to another
# tenant, and whether the account we are logged in as has ever talked to them. Of the eleven
# keys a row carries, the three left out are not facts about the person -- `open_id` is the
# directory's own name, `match_segments` describes the query, and `chat_recency_hint` renders
# the current moment ("Contacted today"), so it is false tomorrow and rewrites every file.
PROFILE_FIELDS = ("localized_name", "enterprise_email", "email", "department", "p2p_chat_id", "is_activated", "is_cross_tenant", "has_chatted")
BATCH = 19  # one under the 20-match cap `+search-user` enforces, measured against resolvable ids


async def sync_profiles(store: Store, p: Progress):
    """Backfill profiles for users already on disk.

    A roster gives one human-readable field, `name`, and it does not identify anyone: two
    active 冯欢 share it exactly and differ only by the alias in `localized_name`.

    `+search-user` caps at 20 *matches*, not 20 ids, and does not paginate -- 20 resolvable
    ids come back as 20 rows with `has_more`, 19 as 19 without one. BATCH stays under the
    cap so the cap is never reached and there is nothing to page past.
    Only own-tenant users are asked for; external ids do not resolve and would spend the
    quota for nothing.
    """
    p.set("profiles", state="running")
    try:
        tenant = (((await cli.run("contact", "+get-user")) or {}).get("user") or {}).get("tenant_key")
    except cli.LarkError as e:
        # `contact:user:search` is a scope a tenant may simply not grant, and this pass is
        # the only one that needs it. Left to propagate it takes `sync_all` down with it,
        # and under `watch` that is the daemon -- one optional scope ending every other
        # collection. A real install hit exactly this.
        p.set("profiles", state="error", note=_reason(e))
        return
    on_disk = {d.name: store.read_yaml(f"users/{d.name}/meta.yaml") for d in (store.root / "users").glob("ou_*")}
    # a resolved profile is not a permanent one: an email, a department and an activation
    # state all change, and "localized_name is present" was reading as "done for good"
    stale = not swept_recently(store, "profiles", PROFILE_HOURS)
    todo = [oid for oid, u in on_disk.items() if u.get("tenant_key") == tenant and (stale or "localized_name" not in u)]
    p.set("profiles", total=len(todo), note=f"{len(on_disk)} known")
    if not todo:
        p.set("profiles", state="done", note="up to date")
        return

    resolved, answered = 0, True

    async def one(batch: list[str]):
        nonlocal resolved, answered
        try:
            d = await cli.run("contact", "+search-user", "--user-ids", ",".join(batch), "--as", "user") or {}
        except cli.LarkError:
            answered = False
            p.bump("profiles", len(batch))  # one batch that would not resolve is not the pass failing
            return
        rows = d.get("users") or []
        for row in rows:
            rel = f"users/{row['open_id']}/meta.yaml"
            # a roster row is the richer one for name/member_id; only the profile-only fields are taken
            store.write_yaml(rel, _clean({**store.read_yaml(rel), **{k: row[k] for k in PROFILE_FIELDS if k in row}}))
        resolved += len(rows)
        p.bump("profiles", len(batch))

    await cli.spread(one, [todo[i : i + BATCH] for i in range(0, len(todo), BATCH)])
    # The clock is claimed by a pass that got answers, not by one that got rows: the 89 ids
    # still outstanding here are deactivated accounts and bots, which resolve to nothing and
    # always will. Gating on `resolved` meant a pass made entirely of those never claimed its
    # week, so the same five requests went out on every single run. A rehired account now
    # appears within the week rather than within the run, which is what the clock is for.
    if stale and answered:
        record_sweep(store, "profiles")
    p.set("profiles", state="done", note=f"{resolved}/{len(todo)} resolved")


def _record_sender(store: Store, msg: dict, known: set[str]):
    """Senders carry their own name and open_id, so a usable user directory can be built
    from messages alone -- `+chat-members-list` needs im:chat:read, which may not be granted."""
    sender = msg.get("sender") or {}
    oid = sender.get("open_bot_id") or (sender.get("id") if sender.get("id_type") == "open_id" else None)
    if not oid or oid in known:
        return
    known.add(oid)
    rel = f"users/{oid}/meta.yaml"
    if not store.exists(rel):
        store.write_yaml(rel, {"open_id": oid, "name": sender.get("name", ""), "sender_type": sender.get("sender_type", ""), "tenant_key": sender.get("tenant_key", "")})


# Drive search returns a ranked slice, not the corpus: an empty query yields far fewer
# hits than a common word does, so coverage comes from unioning several probes.
DOC_QUERIES = ["", "a", "e", "的", "会议", "设计", "项目", "需求", "方案", "数据", "模型", "文档", "记录", "计划"]


def _wiki_aliases(store: Store) -> dict[str, str]:
    """node_token -> obj_token, from the node lists already on disk.

    A search hit of entity_type WIKI carries a *node* token, not the document's own. Both
    work for fetching a body -- lark-cli resolves them -- but Drive answers 1069307 "not
    exist" when asked for comments, and that was a thousand wasted requests every run.
    Translating also merges those hits with the copies the wiki walk found, instead of
    mirroring the same document under two names.
    """
    return {n["node_token"]: n["obj_token"] for n in store.glob_rows("wiki/*/nodes.yaml") if n.get("node_token") and n.get("obj_token")}


def swept_recently(store: Store, name: str, hours: float) -> bool:
    """Has this discovery pass run within `hours`?

    Measured over one full sync: 442 of 1247 requests were the wiki tree walk and 248 were
    the doc search probes -- 55% of the run, spent rediscovering corpora that barely move.
    Neither can be made incremental, and it is not for want of trying: a wiki node carries
    no update time (only token, type, title and has_child) and a search returns a ranked
    slice with no cursor. Frequency is the only variable left.

    An explicit `--only wiki` always sweeps: asking for a collection by name is asking for
    it now, and this only governs what a plain `sync` does on its own initiative.
    """
    last = store.cursors.setdefault("swept", {}).get(name)
    return bool(last) and datetime.fromisoformat(last) > datetime.now(UTC) - timedelta(hours=hours)


def record_sweep(store: Store, name: str):
    """Stamp a pass as swept -- only once it has actually swept.

    Stamping on the way in instead is the same line of code and reads the same, but a pass
    that dies on its first request then coasts the whole window having fetched nothing: one
    rate-limited `+space-list` costs a day of wiki, not a run of it. Failure has to come
    due sooner than success, not at the same time.
    """
    store.cursors.setdefault("swept", {})[name] = datetime.now(UTC).isoformat(timespec="seconds")


async def sync_docs(store: Store, p: Progress, *, queries: list[str] | None = None, search: bool = True):
    """Cloud docs discovered via search and via wiki nodes, exported to markdown, plus comments."""
    p.set("docs", state="running")
    seen: dict[str, dict] = {}
    alias = _wiki_aliases(store)
    found: list[tuple[str, str]] = []  # documents linked from the bodies this run fetched
    probed = search and not queries  # a custom query set sweeps a different corpus; it is not the scheduled pass
    # Sequential on purpose, and it is the one pass that does not fan out. Walking the 14
    # queries end to end runs at ~1.15 req/s, which is under whatever sustained budget the
    # endpoint enforces: measured twice, 14/14 queries answered, 4762 hits, 217s. Spreading
    # them over the shared semaphore is 10x faster and silently wrong -- at width 3 nine
    # queries were cut short by 99991400 and 2840 of those 4762 hits never arrived; at width
    # 8, more. The burst is fine (14 first pages at 8-way, zero 429s); the sustained rate is
    # not. This loop's slowness is the pacing.
    for q in queries or DOC_QUERIES if search else ():
        try:
            async for r in cli.paginate("drive", "+search", "--query", q, key="results"):
                meta = r.get("result_meta") or {}
                if not (token := str(meta.get("token") or "")):
                    continue
                kind = ""
                if r.get("entity_type") == "WIKI":
                    if resolved := alias.get(token):
                        token = resolved  # the node token this hit carries is not the document's own
                    else:
                        # Not in any node list we hold, so the node token is all we will ever
                        # have for it. Drive answers 1069307 for a node token asked for as the
                        # docx it wraps, and answers it as `wiki` -- and 131005 if a resolved
                        # obj_token is asked for that way, so this cannot be applied blanket.
                        kind = "wiki"
                if token in seen:
                    continue
                title = _clean(r.get("title_highlighted"))
                # The one thing in a hit that is not in `result_meta`, and for the 438
                # documents that export no body -- sheets, bitables, mindnotes, the ones
                # nobody shared -- it is the only text of their contents this mirror will
                # ever hold. Query-bound, so a later query's snippet replaces it; `<b>` and
                # `<hb>` are the endpoint's own emphasis markers and are not part of the text.
                summary = cli.unescape_entities(cli.RE_MARKUP.sub("", r.get("summary_highlighted") or ""))
                _note_tenant(meta.get("url", ""))
                # `kind` has to survive to disk: 260 documents are addressable only as `wiki`,
                # and a later run that meets one through the directory instead of a search hit
                # would re-derive `docx` from `doc_types`, get 1069307, and file it as having no
                # comments -- the first one tried this way had four. The row is merged over
                # what is already there for the same reason the wiki pass merges: the two
                # discovery routes describe the same document and know different things.
                seen[token] = {
                    **store.read_yaml(f"docs/{token}/meta.yaml"),
                    **meta,
                    "token": token,
                    "entity_type": r.get("entity_type"),
                    "title": title,
                    **({"summary": summary} if summary else {}),
                    **({"comment_type": kind} if kind else {}),
                }
                store.write_yaml(f"docs/{token}/meta.yaml", seen[token])
                p.bump("docs", last=cli.oneline(title))
        except cli.LarkError:
            probed = False  # cut short: the rest of this query's pages are gone, so the corpus on disk is partial
            continue

    # A sweep that was cut short must not claim its window: coasting six hours on a corpus
    # missing 60% of its hits is worse than paying for the pass again next run, and the
    # limit that caused it clears in seconds. Measured clean, the sequential walk answers
    # 14 of 14, so this is the rare case rather than the usual one.
    if probed and seen:
        record_sweep(store, "docs")

    # wiki nodes point at real documents and are enumerated exhaustively, unlike search
    for node in store.glob_rows("wiki/*/nodes.yaml"):
        if (token := node.get("obj_token")) and node.get("obj_type") in ("docx", "doc", "sheet", "bitable") and token not in seen:
            # Merged, not replaced. This pass runs on every sync and the search above is a
            # ranked slice, so a document the queries missed this time used to have its whole
            # search row -- owner, url, and the `update_time` that decides whether its body is
            # re-exported -- overwritten by these five keys. 2289 of 4140 documents on this
            # store were in that state, which is to say their bodies had stopped refreshing.
            known = {"token": token, "title": node.get("title", ""), "obj_type": node.get("obj_type"), "wiki_node_token": node.get("node_token", ""), "space_id": node.get("space_id", "")}
            seen[token] = {**store.read_yaml(f"docs/{token}/meta.yaml"), **known}
            store.write_yaml(f"docs/{token}/meta.yaml", seen[token])
            p.bump("docs", last=cli.oneline(node.get("title")))

    # A search is a ranked slice, so `seen` is what this run happened to be handed -- not
    # what the mirror knows. A document discovered by an earlier run and not returned by
    # this one was never visited again, whatever state it was left in: 15 sat with a body
    # and no record of ever having been asked for comments, waiting for a slice that might
    # never come back. The directory is the record of everything ever found, so the whole of
    # it is what the rest of this pass works from.
    on_disk = {d.name: store.read_yaml(f"docs/{d.name}/meta.yaml") for d in (store.root / "docs").glob("*") if d.is_dir()}

    # Every document's freshness for 21 requests. `update_time` is the only thing that says a
    # body needs exporting again, and it used to arrive only on a search hit -- so of 4140
    # documents, 3360 had none and their bodies were frozen at whatever the run that first
    # found them exported. `metas batch_query` answers for 200 tokens at a time, whichever
    # pass found them, with the same number the search reports (checked on one holding both).
    async def freshen(batch: list[tuple[str, str]]):
        payload = dumps({"request_docs": [{"doc_token": t, "doc_type": k} for t, k in batch], "with_url": True})
        try:
            metas = (await cli.run("drive", "metas", "batch_query", "--data", payload) or {}).get("metas") or []
        except cli.LarkError:
            return  # a batch that does not answer is a batch of timestamps we already had
        for m in metas:
            token = ((m.get("request_doc_info") or {}).get("doc_token")) or m["doc_token"]
            # `latest_modify_time` is this store's `update_time` under another name, and the
            # `doc_type` it answers with is the *resolved* kind -- `doc_types` already records
            # the kind this token has to be addressed by, and for a wiki token they differ.
            row = {
                "update_time": int(m["latest_modify_time"]),
                "create_time": int(m["create_time"]),
                "owner_id": m.get("owner_id"),
                "latest_modify_user": m.get("latest_modify_user"),
                "title": m.get("title"),
                "url": m.get("url"),
            }
            if (own := m.get("doc_token")) and own != token:
                row["obj_token"] = own  # a wiki token answers as the document it wraps, under the name the node lists use
            on_disk[token] = {**on_disk.get(token, {}), **row}
            store.write_yaml(f"docs/{token}/meta.yaml", on_disk[token])

    asks = [(t, kind.lower()) for t, m in on_disk.items() if (kind := str(m.get("obj_type") or m.get("doc_types") or ""))]
    await cli.spread(freshen, [asks[i : i + META_BATCH] for i in range(0, len(asks), META_BATCH)])
    seen.update(on_disk)

    # bodies are the expensive part: fetch one only if we have none, or if the server's
    # update_time moved past the copy we already wrote. A doc can be edited at any time,
    # so there is no window to bound this -- the timestamp is the only reliable signal.
    todo = [t for t, meta in seen.items() if _doc_is_stale(store, t, meta) or _doc_wants_comments(store, t)]
    p.set("docs", done=0, total=len(todo), note=f"{len(seen)} docs")

    async def body(token: str):
        title = cli.oneline(seen[token].get("title") or token, 48)
        if _doc_is_stale(store, token, seen[token]):
            verdict = ""
            try:
                data = await cli.run("docs", "+fetch", "--doc", token, "--doc-format", "markdown", subject=f"fetch {title}")
            except cli.LarkError as e:
                # only docx exports markdown, a deleted page exports nothing ever again, and
                # a document nobody shared exports nothing today -- all three can still carry
                # comments, so all three keep going
                if not (verdict := "unsupported" if e.is_unsupported_type else "deleted" if e.is_missing else "forbidden" if e.is_forbidden else ""):
                    return  # transient: leave it due, the next run retries it
                data = None
            content = ((data or {}).get("document") or {}).get("content")
            if content:
                store.write(f"docs/{token}/content.md", content)
                found.extend(_doc_links(content))  # documents point at documents, and 495 of those were nowhere else
            else:
                # An empty body now is not an empty body forever: a docx written later has
                # content where it had none, and a bare marker outlasts it. Only "unsupported"
                # is a permanent verdict; the marker records which of the two this was, and its
                # own mtime is what an empty one is re-checked against.
                store.write(f"docs/{token}/.nobody", verdict)
        if not _doc_wants_comments(store, token):
            return
        # asking as `docx` is what Drive recognises the token *as*, and anything that is not
        # one answers 1069307 -- an error we then remember as "has no comments". 220 sheets
        # and bitables from the wiki, and 203 more from search, lost their comments that way,
        # to a mistake of our own. A single call also stops at one page, and nine documents
        # sat at exactly 50 with `has_more` set -- the busiest ones, which are the ones worth
        # having; asking the first of them for its second page returned 33 more comments.
        kind = _doc_type(seen[token])
        for retried in (False, True):
            try:
                comments = [c async for c in cli.paginate("drive", "+list-comments", "--token", token, "--type", kind, "--solved-status", "all", key="items")]
                break
            except cli.LarkError as e:
                # 131005 is Drive saying this token is real and is not a wiki node, so the
                # `/wiki/` link that named it carried the document's own token rather than
                # its node's. Nothing in the link says which, and 162 documents stalled on
                # it -- retried every run, recorded never. One retry settles it, and writing
                # the answer into the meta is what keeps it to one.
                if not retried and kind == "wiki" and e.is_wrong_kind:
                    kind = "docx"
                    seen[token]["comment_type"] = kind
                    store.write_yaml(f"docs/{token}/meta.yaml", {**store.read_yaml(f"docs/{token}/meta.yaml"), "comment_type": kind})
                    continue
                # a token Drive does not recognise never will, and neither does a kind the
                # endpoint cannot name; anything else may just be a rate limit, and marking
                # that would lose the comments for good
                if e.is_missing or e.is_unlistable_type:
                    store.write(f"docs/{token}/.nocomments", "")
                elif e.is_forbidden:
                    store.write(f"docs/{token}/.nocomments", "forbidden")
                return
        # The answer is the record, empty or not. Writing only a non-empty one left the
        # document indistinguishable from one never asked, and the retry was gated on the
        # *body* being stale -- so 258 documents whose bodies had settled were never going
        # to be asked again.
        store.write_yaml(f"docs/{token}/comments.yaml", {"count": len(comments), "items": comments})

    async def counted(token: str):
        # once per document and whatever it owed, not once per fetch: `todo` holds documents
        # that only owe comments too, and counting only fetches left the fraction short of
        # its own total -- 33 of 221 on a run where every one of the 221 was visited
        try:
            await body(token)
        finally:
            p.bump("docs", last=cli.oneline(seen[token].get("title") or token, 48))

    await cli.spread(counted, todo)
    _flush_doc_links(store, found)
    p.set("docs", state="done", note=f"{len(seen)} docs")


# What `drive +list-comments --type` will name. A type outside this set is not a soft
# failure: Drive answers 1069307, the same code that means "no such token", and the caller
# remembers that as "has no comments" forever. `mindnote` is deliberately absent -- the
# endpoint does not name it, and asking as `docx` instead is the closest thing available.
DOC_TYPES = {"doc", "docx", "sheet", "file", "slides", "bitable", "base", "apps", "wiki"}


def _doc_type(meta: dict) -> str:
    """What Drive should be asked to recognise this token as.

    A wiki node calls it `obj_type` and a search hit calls it `doc_types`, in upper case;
    the two never appear on the same record. Measured with real tokens of each kind: asked
    as itself every one answers, asked as `docx` every one answers 1069307.

    `comment_type` overrides both, and only the search loop sets it: a wiki hit whose node
    token it could not resolve is addressed as `wiki`, because that is the only name Drive
    has for it. It is not a property of the document -- the same document resolved answers
    as its own type and answers 131005 as `wiki` -- so it cannot be derived here.
    """
    kind = str(meta.get("comment_type") or meta.get("obj_type") or meta.get("doc_types") or meta.get("file_type") or "").lower()
    return kind if kind in DOC_TYPES else "docx"


def _doc_wants_comments(store: Store, token: str) -> bool:
    """True when this document's comments are missing, or old enough to ask about again.

    The fetch used to sit inside the body pass, which only runs for a document whose body
    is stale -- so one transient failure was permanent: the body settled, the pass stopped
    visiting, and the comments were never asked for again. 258 documents on this store, 8%
    of the corpus, had a body and no record at all.

    Once recorded they then never moved, and there is no signal that would tell us to look:
    commenting does not touch `update_time`, and 26 of the 403 documents that have comments
    carry one newer than the body's own timestamp. So the record's mtime is the clock, the
    way `.nobody`'s is. Reading it costs a parse, which is why the cheap stat runs first --
    the answer for all but a handful of documents on any given run is no.
    """
    if (never := store.root / f"docs/{token}/.nocomments").exists():
        # empty is a verdict -- Drive does not know this token, or cannot name its kind.
        # "forbidden" is only today's answer, so it is re-asked on the same clock a locked
        # body is: without one, a document nobody shared was a request on every single run.
        return never.read_text() == "forbidden" and datetime.now(UTC).timestamp() - never.stat().st_mtime > NOACCESS_HOURS * 3600
    record = store.root / f"docs/{token}/comments.yaml"
    if not record.exists():
        return True
    age = datetime.now(UTC).timestamp() - record.stat().st_mtime
    if age < COMMENT_HOURS * 3600:
        return False
    return age > COMMENT_EMPTY_HOURS * 3600 or bool(store.read_yaml(f"docs/{token}/comments.yaml").get("items"))


def _doc_is_stale(store: Store, token: str, meta: dict) -> bool:
    """True when the body is missing, or the server copy is newer than ours.

    A `.nobody` marker stands in for the body it does not have, and its content says which
    kind of nothing: "unsupported" and "deleted" are permanent -- only docx exports markdown,
    and a deleted page is gone -- "forbidden" is a document nobody shared, which they still
    might, so it is a clock; and an empty one only means the document was empty when we last
    looked, with its mtime as when that was. Treating them alike froze an empty docx out of
    every later run, and re-fetched an unreachable one on every single sync.
    """
    remote = meta.get("update_time")
    body = store.root / f"docs/{token}/content.md"
    if body.exists():
        return bool(remote and remote > body.stat().st_mtime)
    mark = store.root / f"docs/{token}/.nobody"
    if not mark.exists():
        return True
    if (verdict := mark.read_text()) in ("unsupported", "deleted"):
        return False
    if verdict == "forbidden":
        return datetime.now(UTC).timestamp() - mark.stat().st_mtime > NOACCESS_HOURS * 3600
    return bool(remote and remote > mark.stat().st_mtime)


def _edit_signal(msg: dict) -> tuple:
    """What actually changes when a message is edited."""
    return (msg.get("update_time") or "", str(msg.get("content") or ""), bool(msg.get("updated")))


async def recheck_messages(store: Store, p: Progress, *, window_days: int = 30, batch: int = 50, budget: int = 60):
    """Re-verify already-synced messages, since edits and recalls leave no forward trace.

    `+messages-search` cannot filter by update time and never returns recalled messages,
    but `+messages-mget` reports `update_time` per id. So we replay ids we already have and
    rewrite only what actually changed -- one request per 50 messages, far cheaper than
    re-fetching them.

    `budget` batches per run, resumed from where the last one stopped. Replaying the whole
    window every time was 2431 sequential requests on this store -- longer than the 30
    minutes until the tier is due again, so the daemon never left it, at roughly 115k
    requests a day against a monthly tenant quota. The cursor is the last id read rather
    than an offset, because the id list shifts as messages arrive and age out.

    Thread replies are included. They have no month directory to filter on, so the window
    only narrows the main line; the cursor is what bounds the work either way.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=window_days)).strftime("%Y-%m")
    files = [f for f in (store.root / "chats").glob("*/messages/*/*.yaml") if f.parent.name >= cutoff]
    files += [f for f in (store.root / "chats").glob("*/threads/*/*.yaml") if f.stem != "meta"]
    if not files:
        return

    by_id = {f.stem: f for f in files}
    half = budget * batch // 2
    # Up to half the budget on what this mirror wrote most recently, because that is where an
    # edit lands -- usually within minutes of the message. Message ids do not encode that:
    # measured over the real window their sort order runs *against* create_time (Kendall tau
    # -0.73), so a cursor alone reaches a new message at a random point in its cycle, 15
    # hours in on average, against the 30 minutes this used to take.
    #
    # A window rather than a top-N: this pass rewrites what it finds edited, which resets
    # the very mtime it sorts on, so a top-N would keep re-picking whatever it just touched.
    # An hour of a sweep that runs every two minutes is a small, self-limiting set.
    fresh = datetime.now(UTC).timestamp() - RECHECK_FRESH_SECONDS
    hot = sorted((f.stat().st_mtime, f.stem) for f in files if f.stat().st_mtime >= fresh)[-half:]
    hot = [mid for _, mid in hot]
    # The other half sweeps everything else, resumed from the last id read rather than an
    # offset -- the list shifts as messages arrive and age out.
    room = budget * batch - len(hot)  # whatever the hot half did not need; an hour is usually far under it
    rest = sorted(set(by_id) - set(hot))
    start = bisect_right(rest, store.cursors.get("recheck_after") or "")
    cold = rest[start : start + room] or rest[:room]  # wrapped: begin again at the start
    slice_ = sorted(set(hot) | set(cold))
    p.set("recheck", state="running", total=len(slice_), done=0, note=f"{len(by_id)} in window")

    # written by an earlier slice, and this one only sees its own ids -- rewriting the file
    # from what came back here would erase every recall recorded before it
    gone = {r["message_id"]: r for r in store.read_yaml_rows("recalled.yaml") if r.get("message_id")}
    changed = 0
    for i in range(0, len(slice_), batch):
        chunk = slice_[i : i + batch]
        try:
            # the only path that keeps `--no-reactions`: it re-reads 3000 already-known
            # messages a run, and the merge below leaves whatever the walk already recorded
            data = await cli.run("im", "+messages-mget", "--message-ids", ",".join(chunk), "--no-reactions")
        except cli.LarkError:
            p.bump("recheck", len(chunk))
            continue
        returned = {m["message_id"]: m for m in (data or {}).get("messages") or []}
        for mid in chunk:
            if not (msg := returned.get(mid)):
                # recalled: it is gone from the server, but the local copy is the only
                # record left, so mark it rather than delete it
                gone[mid] = {"message_id": mid, "path": str(by_id[mid].relative_to(store.root))}
                continue
            gone.pop(mid, None)  # it answered: a record of its absence must not outlive it
            # mget returns a wider field set than search, so comparing whole payloads
            # would rewrite every file; the edit signal is update_time plus the body
            rel = str(by_id[mid].relative_to(store.root))
            before = store.read_yaml(rel)
            if _edit_signal(msg) != _edit_signal(before):
                merged = {**before, **_clean(msg)}
                store.write_yaml(rel, merged)
                changed += 1
        p.bump("recheck", len(chunk), last=f"{changed} changed, {len(gone)} recalled")
    store.write_yaml("recalled.yaml", [gone[k] for k in sorted(gone)])
    store.cursors["recheck_after"] = cold[-1] if cold else ""  # the sweeping half owns the cursor; the hot half is not in order
    store.save_cursor("recheck_after")  # the daemon holds this Store from startup; a full save would undo everything the sync advanced since
    p.set("recheck", state="done", note=f"{changed} updated, {len(gone)} recalled, {len(by_id)} in window")


def _rows(payload: dict) -> list[dict]:
    """+record-list returns a column matrix; zip it back into named rows keyed by record_id."""
    fields, data, ids = payload.get("fields") or [], payload.get("data") or [], payload.get("record_id_list") or []
    return [{"record_id": rid, **dict(zip(fields, row, strict=False))} for rid, row in zip(ids, data, strict=False)]


def _schema(payload: dict) -> dict:
    """The half of a +record-list response that describes the table rather than the page.

    Column names alone are not a schema: `field_id_list` is the id that survives a rename,
    `field_type_list` says how to read a cell, and `timezone` is what a date cell is relative
    to. `rev` is the table's revision -- the only thing that will ever say a pull has gone out
    of date. Of the nine keys a response carries, the two left out are the request coming back:
    `query_context` echoes the scope asked for, and `has_more` is consumed by the walk itself.
    """
    cols = zip(payload.get("fields") or [], payload.get("field_id_list") or [], payload.get("field_type_list") or [], strict=False)
    return {"columns": [{"name": n, "id": i, "type": t} for n, i, t in cols], "rev": payload.get("rev"), "timezone": payload.get("timezone")}


def _clean(value) -> Any:
    """Lark's search endpoints return HTML-entity-escaped text with <h> hit markers.
    Left as-is those become `&lt;b&gt;` on disk, which breaks plain-text grep."""
    if isinstance(value, str):
        return cli.unescape_entities(value.replace("<h>", "").replace("</h>", ""))
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


MINUTES_CAP = 50  # `+search` returns at most this many per query, however its pages are walked
MEETINGS_CAP = 150


async def _sweep_window(lo: datetime, hi: datetime, cap: int, take: Callable[[datetime, datetime], Awaitable[int]]):
    """Walk [lo, hi) with `take`, halving any window that comes back at its ceiling.

    The cap is on the *total* a query can return, not on a page, so a window that reaches it
    is not an answer -- it is the first `cap` of an unknown number, and the rest cannot be
    reached through that query at all. A month was assumed narrow enough; it is not.
    Measured on disk: 159 meetings in 2026-07 and 152 in 2026-06, against a ceiling of 150.

    Splitting stops at a day, which the busiest month on record puts at ~5 against 150.
    """
    if await take(lo, hi) < cap:
        return
    mid = (lo + (hi - lo) / 2).replace(hour=0, minute=0, second=0, microsecond=0)
    if mid <= lo or mid >= hi:
        return  # a single day that still fills the cap cannot be split any finer than the API accepts
    await _sweep_window(lo, mid, cap, take)
    await _sweep_window(mid, hi, cap, take)


def _minute_is_due(store: Store, token: str) -> bool:
    """True when the record is short of what the two endpoints return, and worth asking again.

    `owner_id` comes only from `minutes.get`, so its absence names a minute still carrying
    the three fields the old `+detail` shortcut chose to emit -- 553 of them here, and the
    owner, duration, cover, url and keywords arrive on the backfill.

    189 of the 710 minutes the meetings name answer "No read permission", and nothing on
    disk said so -- they were fetched again on every sync, forever. Access can be granted
    later, so the marker is a clock rather than a verdict, the way an empty `.nobody` is.
    """
    if store.exists(f"minutes/{token}/transcript.txt") and "owner_id" in store.read_yaml(f"minutes/{token}/meta.yaml"):
        return False
    mark = store.root / f"minutes/{token}/.noaccess"
    return not mark.exists() or datetime.now(UTC).timestamp() - mark.stat().st_mtime > NOACCESS_HOURS * 3600


async def sync_minutes(store: Store, p: Progress, *, since: str = "", full: bool = True):
    """Minutes (妙记): metadata, AI summary, chapters, todos, and full transcript.

    `+search` caps at 50 results per query no matter the filters, so a single call over
    the whole history silently reports 50. Walking month by month lifts the ceiling.
    """
    p.set("minutes", state="running")
    found: dict[str, dict] = {}
    now = datetime.now(UTC)
    month = _earliest(store, "minutes", since) if full else _recent(now)
    p.set("minutes", total=_months_between(month, now))
    scanned = 0

    async def take(lo: datetime, hi: datetime) -> int:
        seen = 0
        try:
            async for item in cli.paginate("minutes", "+search", "--start", lo.strftime("%Y-%m-%d"), "--end", hi.strftime("%Y-%m-%d"), key="items", page_size=SEARCH_PAGE):
                seen += 1
                if token := item.get("token"):
                    found[token] = item
        except cli.LarkError as e:
            if not e.is_pagination_exhausted:
                raise
        return seen

    while month < now:
        nxt = (month + timedelta(days=32)).replace(day=1)
        try:
            await _sweep_window(month, nxt, MINUTES_CAP, take)
        except cli.LarkError as e:
            p.set("minutes", state="error", note=_reason(e))
            return
        scanned += 1
        p.set("minutes", done=scanned, note=f"scanning {month:%Y-%m} · {len(found)} found")
        month = nxt

    for token, item in found.items():
        store.write_yaml(f"minutes/{token}/meta.yaml", {**store.read_yaml(f"minutes/{token}/meta.yaml"), **_clean(item)})

    # A recorded meeting names its minute, and `+search` did not always rank it: 191 of the
    # 710 minute tokens in meetings/*/detail.yaml had nothing under minutes/ at all. The
    # token is all `+detail` needs, so nothing is written for one until it answers.
    known = {*found} | {m[1] for f in (store.root / "meetings").glob("*.yaml") for m in RE_MINUTE_TOKEN.finditer(f.read_text())}
    todo = [t for t in sorted(known) if _minute_is_due(store, t)]
    p.set("minutes", done=0, total=len(todo), note=f"fetching transcripts · {len(known)} minutes")

    async def detail(token: str):
        """`minutes +detail` is these same two calls -- `--dry-run` prints both URLs -- and it
        emits three fields out of the eight the first one returns, dropping the owner (an
        open_id, the form this store links by), the duration, the create time, the cover and
        the minute's own url. Its `artifacts` half it renames and then drops `keywords` from.
        Asking the two endpoints directly costs the same two requests and keeps all of it.
        """
        Aborted.check()
        try:
            base = await cli.run("minutes", "minutes", "get", "--minute-token", token)
        except cli.LarkError as e:
            if e.is_forbidden:
                store.write(f"minutes/{token}/.noaccess", "")  # not permanent -- access can be granted, so the marker's mtime is when to look again
            return
        finally:
            p.bump("minutes", last=cli.summarise_title((found.get(token) or {}).get("display_info")))
        try:
            arts = await cli.run("api", "GET", f"/open-apis/minutes/v1/minutes/{token}/artifacts", "--params", ARTIFACTS) or {}
        except cli.LarkError:
            arts = {}  # the AI products are the half that can be missing; the minute itself is not
        # the two prose artifacts stay their own files: a transcript read through a YAML block
        # scalar is not what `rg` is for. Everything else is one record.
        for key, name in (("summary", "summary.md"), ("transcript", "transcript.txt")):
            if text := arts.pop(key, None):
                store.write(f"minutes/{token}/{name}", text if isinstance(text, str) else str(text))
        card = {k: v for k, v in store.read_yaml(f"minutes/{token}/meta.yaml").items() if k in MINUTE_CARD_KEYS}
        store.write_yaml(f"minutes/{token}/meta.yaml", {**card, **_clean((base or {}).get("minute") or {}), **_clean(arts)})

    await cli.spread(detail, todo)
    if full:
        record_sweep(store, "minutes")
    p.set("minutes", state="done", note=f"{len(known)} minutes")


async def sync_meetings(store: Store, p: Progress, *, since: str = "", full: bool = True):
    """VC meeting records; links each meeting to its minute_token and note_id.

    Like minutes, `+search` has a per-query ceiling (150 here), so walk month by month.
    """
    p.set("meetings", state="running")
    ids: set[str] = set()  # a window that splits is walked again from its start, so this cannot be a list
    now = datetime.now(UTC)
    month = _earliest(store, "meetings", since, "*.yaml") if full else _recent(now)
    p.set("meetings", total=_months_between(month, now))
    scanned = 0

    async def take(lo: datetime, hi: datetime) -> int:
        seen = 0
        try:
            async for item in cli.paginate("vc", "+search", "--start", lo.strftime("%Y-%m-%d"), "--end", hi.strftime("%Y-%m-%d"), key="items", page_size=SEARCH_PAGE):
                seen += 1
                if mid := item.get("id"):
                    ids.add(mid)
                    # the card is kept, not derived away: everything in it looks reconstructible
                    # from the structured half -- title, time, meeting number, the organiser as
                    # an open_id -- but "looks reconstructible" is a judgement, and the rendered
                    # form is 233 bytes. The two writers own disjoint keys, so each replaces its
                    # own and leaves the other's alone.
                    store.write_yaml(f"meetings/{mid}.yaml", {**store.read_yaml(f"meetings/{mid}.yaml"), **_clean(item)})
        except cli.LarkError as e:
            if not e.is_pagination_exhausted:
                raise
        return seen

    while month < now:
        nxt = (month + timedelta(days=32)).replace(day=1)
        try:
            await _sweep_window(month, nxt, MEETINGS_CAP, take)
        except cli.LarkError as e:
            p.set("meetings", state="error", note=_reason(e))
            return
        scanned += 1
        p.set("meetings", done=scanned, note=f"scanning {month:%Y-%m} · {len(ids)} found")
        month = nxt

    todo = [i for i in sorted(ids) if _meeting_is_due(store, i)]
    p.set("meetings", done=0, total=len(todo), note=f"fetching details · {len(ids)} meetings")
    recordings: dict[str, dict] = {}

    async def recording(batch: list[str]):
        """The minute half. Optional: a meeting nobody recorded is still worth writing down."""
        try:
            data = await cli.run("vc", "+recording", "--meeting-ids", ",".join(batch))
        except cli.LarkError:
            return
        for r in (data or {}).get("recordings") or []:
            if mid := r.get("meeting_id"):
                recordings[mid] = {k: v for k, v in r.items() if k != "meeting_id"}

    async def detail(mid: str):
        try:
            data = await cli.run("vc", "meeting", "get", "--meeting-id", mid, "--with-participants")
        except cli.LarkError:
            return  # transient: leave it due, the next run retries it
        finally:
            p.bump("meetings")
        if meeting := (data or {}).get("meeting") or {}:
            card = {k: v for k, v in store.read_yaml(f"meetings/{mid}.yaml").items() if k in MEETING_CARD_KEYS}
            store.write_yaml(f"meetings/{mid}.yaml", {**card, **_clean(_meeting_row(meeting, recordings.get(mid) or {}))})

    # recordings first, so a meeting is written once with both halves in it
    await cli.spread(recording, [todo[i : i + 20] for i in range(0, len(todo), 20)])
    await cli.spread(detail, todo)
    await _resolve_notes(store, p)
    if full:
        record_sweep(store, "meetings")
    p.set("meetings", state="done", note=f"{len(ids)} meetings")


RE_NOTE_ID = compile(r"^note_id: '?(\d+)'?$", MULTILINE)


async def _resolve_notes(store: Store, p: Progress):
    """Turn the note a meeting kept into the documents it actually is.

    `note_id` is reported by `+detail` and is not a drive token -- nothing can be fetched
    with it. `note +detail` translates it into two or three that can be: the note itself,
    the verbatim transcript, and whatever was shared into the meeting. None of the 131 notes
    on this store had any of its documents mirrored, because no search ever ranked them.

    Asked once per note: the mapping is a fact about a meeting that already happened, so the
    answer on disk is the reason not to ask again.
    """
    # a meeting is one file and a minute is a directory of them, so the two shapes are listed apart
    files = [*(store.root / "meetings").glob("*.yaml"), *(store.root / "minutes").glob("*/detail.yaml")]
    seen = {m[1] for f in files for m in RE_NOTE_ID.finditer(f.read_text())}
    todo = sorted(n for n in seen if not store.exists(f"notes/{n}.yaml"))
    if not todo:
        return
    p.set("meetings", note=f"resolving {len(todo)} meeting notes")
    links: list[tuple[str, str]] = []

    async def one(note_id: str):
        try:
            data = await cli.run("note", "+detail", "--note-id", note_id)
        except cli.LarkError as e:
            if e.is_forbidden:
                # one note on this store, and unlike a document it will not be shared later:
                # the meeting is over. Recording the refusal is what stops the daily retry.
                store.write_yaml(f"notes/{note_id}.yaml", {"note_id": note_id, "forbidden": True})
            return  # anything else is transient: leave it unresolved, the next run asks again
        note = (data or {}).get("note") or {}
        store.write_yaml(f"notes/{note_id}.yaml", _clean(note))
        links.extend((t, "docx") for t in (note.get("note_doc_token"), note.get("verbatim_doc_token"), *(note.get("shared_doc_tokens") or [])) if t)

    await cli.spread(one, todo)
    _flush_doc_links(store, links)


# The three fields `vc meeting get` returns as unix seconds. `+detail` used to format these
# for us, and two passes read the formatted shape -- `_earliest` through RE_START and
# `_meeting_is_due` by comparing against a STAMP string -- so this has to happen here now.
MEETING_EPOCHS = ("create_time", "start_time", "end_time")
# What only `vc +search` returns. Everything else in the record belongs to the structured
# half, which replaces its own keys wholesale -- a field that stops coming back has to
# disappear from disk, and `hint` is one that means something by being absent.
MEETING_CARD_KEYS = ("display_info", "meta_data")
PARTICIPANT_EPOCHS = ("first_join_time", "final_leave_time")


def _stamp(epoch) -> str:
    return datetime.fromtimestamp(int(epoch), TENANT_TZ).strftime(STAMP)


def _meeting_row(meeting: dict, recording: dict) -> dict:
    """One meeting, as the two calls that describe it return it.

    `vc +detail` is those same two calls -- `--dry-run` prints its own steps as
    "meeting.get -> note_id + recording API -> minute_token" -- and it emits six fields out
    of the twenty they hand back. Dropped on the floor: who hosted it, its status, how many
    people came, how long the recording is and where it lives, and the participant list
    entirely, which `+detail` never even asks for. `with_participants` is a query parameter
    on a request the mirror was already paying for, so all of it is free.

    Participants are a field and not a file: unlike comments, records, nodes or a chat
    roster, they arrive inside this same response. There is no state where the mirror has
    the meeting and not the people in it, so a file boundary there would mean nothing.
    """
    people = meeting.pop("participants", None) or []
    row = {**meeting, **recording}
    return {
        **{k: (_stamp(v) if k in MEETING_EPOCHS and v else v) for k, v in row.items()},
        "participants": [{k: (_stamp(v) if k in PARTICIPANT_EPOCHS and v else v) for k, v in one.items()} for one in people],
    }


def _meeting_is_due(store: Store, mid: str) -> bool:
    """True when there is nothing on disk yet, or what is there was taken too early.

    `minute_token` and `note_id` appear only once the recording has been processed, which
    is after the meeting ends -- so a record fetched while it was still running never has
    them, and "the file exists" froze that. Only a meeting recent enough to still be
    waiting is worth another look: measured, 133 of 842 have no minute at all and the API
    says so outright, so asking those again buys nothing forever.

    A record with no `participants` key predates the mirror asking for them, and is due for
    that reason alone -- which is what backfills the 849 written by the old `+detail` shape.
    """
    row = store.read_yaml(f"meetings/{mid}.yaml")
    if not row or "participants" not in row:
        return True
    if row.get("minute_token") or row.get("note_id"):
        return False
    end = str(row.get("end_time") or "")
    return bool(end and end > (datetime.now(TENANT_TZ) - timedelta(days=MEETING_SETTLE_DAYS)).strftime(STAMP))


async def sync_bases(store: Store, p: Progress):
    """Bitables reachable from drive search; one JSONL of records per table."""
    p.set("bases", state="running")
    tokens: set[str] = set()
    try:
        async for r in cli.paginate("drive", "+search", "--query", "", key="results"):
            meta = r.get("result_meta") or {}
            if "BITABLE" in (meta.get("doc_types") or "") and (t := meta.get("token")):
                tokens.add(t)
    except cli.LarkError:
        pass

    # One empty query is one ranked slice: it found 11 of the 178 bitables the document
    # pass already has on disk, which asks 14 queries and writes down what each hit was.
    # The type is a plain scalar, so it is matched in the text rather than parsed -- 0.6s
    # across 3049 files against 3.6s, for an answer that was identical on every one.
    tokens |= {f.parent.name for f in (store.root / "docs").glob("*/meta.yaml") if RE_BITABLE.search(f.read_text())}

    # Listing a base is a request per base, so 178 of them is a whole sync's worth on every
    # run. Nothing about a bitable says when it last changed, so it is a clock like the rest.
    stale = not swept_recently(store, "bases", BASE_HOURS)
    todo = [t for t in tokens if stale or not store.exists(f"bases/{t}")]
    p.set("bases", total=len(todo), note=f"{len(tokens)} bases")

    whole_pass = True

    async def one(app_token: str):
        nonlocal whole_pass
        try:
            tables = await cli.run("base", "+table-list", "--base-token", app_token, "--limit", "100")
        except cli.LarkError as e:
            # a token that is not a base will never be one, and two of the ones the document
            # corpus names are exactly that -- counting them as "cut short" meant the day was
            # never claimed and all 185 were listed again on every single run
            whole_pass = whole_pass and e.is_missing
            return
        for t in (tables or {}).get("tables") or []:
            tid = t.get("id")
            head = f"bases/{app_token}/tables/{tid}/meta.yaml"
            store.write_yaml(head, {**store.read_yaml(head), **t})
            rel = f"bases/{app_token}/tables/{tid}/records.yaml"
            pulled = store.exists(rel)  # records are re-pulled only on demand; full-table diffing is a later concern
            if pulled and "columns" in store.read_yaml(head):
                continue
            # +record-list is offset-based (not page-token) and defaults to a markdown table.
            # A table whose records are already down is asked for one row, because the header
            # of any page carries the schema and that is the only thing still missing.
            rows: list[dict] = []
            whole = True
            while True:
                try:
                    recs = await cli.run("base", "+record-list", "--base-token", app_token, "--table-id", tid, "--limit", "1" if pulled else "200", "--offset", str(len(rows))) or {}
                except cli.LarkError:
                    whole = False
                    break
                if not rows:
                    store.write_yaml(head, {**store.read_yaml(head), **_schema(recs)})
                    if pulled:
                        break
                rows += _rows(recs)
                if not recs.get("has_more"):
                    break
            # A half table written here is permanent -- `store.exists(rel)` above skips the
            # file forever after, so one rate-limited page in the middle freezes the first
            # 400 rows in place as if they were the whole thing.
            if whole and not pulled:
                store.write_yaml(rel, _clean(rows))
        p.bump("bases")

    await cli.spread(one, todo)
    # a pass that failed halfway must not claim its day, for the reason the doc search gives
    if stale and todo and whole_pass:
        record_sweep(store, "bases")
    p.set("bases", state="done", note=f"{len(tokens)} bases")


async def sync_wiki(store: Store, p: Progress):
    p.set("wiki", state="running")
    try:
        spaces = await cli.run("wiki", "+space-list", "--page-all", "--page-limit", "0")
    except cli.LarkError as e:
        p.set("wiki", state="error", note=str(e.payload)[:60])
        return
    items = (spaces or {}).get("spaces") or []
    p.set("wiki", note=f"{len(items)} spaces")

    async def level(sid: str, parent: str | None) -> list[dict] | None:
        """One level of the tree, or None when the request failed.

        The distinction matters: a leaf and a rate-limited branch both have no nodes to
        return, and conflating them lets a failure look like an answer.
        """
        # `--page-limit 0` for the reason given at the roster call: ten pages is the default
        # ceiling, and no space or level is near it today, but a silent one is worth removing
        argv = ["wiki", "+node-list", "--space-id", sid, "--page-all", "--page-limit", "0", *(["--parent-node-token", parent] if parent else [])]
        try:
            return ((await cli.run(*argv)) or {}).get("nodes") or []
        except cli.LarkError:
            return None

    async def walk(sid: str) -> tuple[list[dict], bool]:
        """Wiki nodes form a tree and `+node-list` returns one level at a time.

        Breadth-first with a bounded fan-out per round: recursing with `gather` queues the
        whole subtree at once, which floods the API into a rate limit even though the
        semaphore caps what runs concurrently. Also reports whether every level answered.
        """
        found: list[dict] = []
        whole = True
        frontier: list[str | None] = [None]
        while frontier:
            batch, frontier = frontier[:WIKI_WIDTH], frontier[WIKI_WIDTH:]
            for nodes in await gather(*(level(sid, parent) for parent in batch)):
                if nodes is None:
                    whole = False
                    continue
                found += nodes
                frontier += [n["node_token"] for n in nodes if n.get("has_child")]
            # the tree's size is only known as it is walked, so report nodes against the
            # frontier still queued -- a space count would sit at 0/1 for the whole sweep
            p.set("wiki", done=len(found), total=len(found) + len(frontier), note=f"{len(frontier)} branches queued")
        return found, whole

    complete = True

    async def one(space: dict):
        nonlocal complete
        if not (sid := space.get("space_id")):
            return
        store.write_yaml(f"wiki/{sid}/meta.yaml", space)
        nodes, whole = await walk(sid)
        complete &= whole
        rel = f"wiki/{sid}/nodes.yaml"
        # A partial walk must not overwrite a complete one. This node list is the only
        # place a wiki node_token can be resolved to the document it points at, and one
        # rate-limited level used to be enough to replace the whole thing with `[]`.
        if whole or not store.exists(rel):
            store.write_yaml(rel, _clean(nodes))
        p.set("wiki", note=f"{len(nodes)} nodes" + ("" if whole else ", partial -- kept the copy on disk"))

    await cli.spread(one, items)
    # A partial walk already refuses to overwrite the copy on disk; it must not claim the
    # window either. WIKI_WIDTH is set where this walks all 2366 nodes with zero 429s, so a
    # partial one means something went wrong -- not the ordinary outcome to be tolerated.
    if complete:
        record_sweep(store, "wiki")
    p.set("wiki", state="done")


ALL = ["messages", "chats", "profiles", "docs", "minutes", "meetings", "bases", "wiki", "files"]


async def sync_all(root: Path, p: Progress, only: list[str] | None = None):
    """Run every collection concurrently.

    The edges are between the values that are actually needed, not between the passes that
    happen to produce them: the chat roster is one request, so both the message sweep and
    the roster pass await *it* rather than one of them awaiting the other. Two real waits
    remain -- files wants the media index the sweep writes at its end, and docs mines
    wiki's node list -- and both are end-of-pass by nature.

    None of this buys throughput on its own: one global semaphore of CONCURRENCY is the
    real scheduler, and a saturated sweep already holds every slot. What it buys is that a
    cheap pass reports promptly instead of after a sweep, and does its work before the
    rate limit that will eventually interrupt one.
    """
    store = Store(root)
    _learn_tenant(store)
    migrate_threads(store)
    migrate_meetings(store)
    migrate_minutes(store)
    want = set(only or ALL)
    named: dict[str, Any] = {}  # by the row each one reports under, so a failure lands where it is drawn

    # The roster is one request, and it is all the chat pass ever wanted. It used to be
    # taken from the message sweep's return value -- which meant waiting out a sweep of
    # every message in every chat for a set that sync_messages computes on its first line.
    roster = create_task(_list_chats(store, "messages" if "messages" in want else "chats")) if want & {"messages", "chats"} else None

    async def walk_messages():
        return await sync_messages(store, p, chat_ids=await roster if roster else None)

    messages = create_task(walk_messages()) if "messages" in want else None
    # `only` names what the caller asked for; a plain sync asks for everything, and that is
    # the case where the discovery passes are allowed to skip a turn (see swept_recently)
    asked = set(only or ())
    walk_wiki = "wiki" in want and ("wiki" in asked or not swept_recently(store, "wiki", WIKI_HOURS))
    wiki = create_task(sync_wiki(store, p)) if walk_wiki else None

    async def chats():
        # depends on the roster, not on the sweep, so a rate-limited sweep cannot take it down
        await sync_chat_meta(store, p, await roster if roster else set())

    rosters = create_task(chats()) if "chats" in want else None

    async def profiles():
        # the roster pass is what puts users on disk; alone this backfills whoever is
        # already there, which is what `--only profiles` is for
        if rosters:
            await rosters
        await sync_profiles(store, p)

    async def docs():
        if wiki:
            await wiki
        # the body fetch below it is already incremental; only the search probes are not
        await sync_docs(store, p, search="docs" in asked or not swept_recently(store, "docs", SEARCH_HOURS))

    async def files():
        # the media index is a by-product of the message sweep; on its own it reads
        # whatever is already on disk, which is what `--only files` is for
        if messages:
            await messages
        await sync_attachments(store, p)

    if "profiles" in want:
        named["profiles"] = profiles()
    if "docs" in want:
        named["docs"] = docs()
    if "files" in want:
        named["files"] = files()
    if "minutes" in want:
        named["minutes"] = sync_minutes(store, p, full="minutes" in asked or not swept_recently(store, "minutes", SEARCH_HOURS))
    if "meetings" in want:
        named["meetings"] = sync_meetings(store, p, full="meetings" in asked or not swept_recently(store, "meetings", SEARCH_HOURS))
    if "bases" in want:
        named["bases"] = sync_bases(store, p)

    # A producer is also awaited here. A task can be awaited more than once, so this costs
    # nothing and removes the need to keep "who else awaits this" in step with the graph --
    # get that wrong and the task is simply abandoned when gather returns.
    named |= {name: t for name, t in (("messages", messages), ("wiki", wiki), ("chats", rosters)) if t}
    if roster:
        named["roster"] = roster

    # One collection failing is not the sync failing. Without this a single LarkError that
    # nothing caught -- `sync_profiles` raised on a scope a tenant had not granted, which a
    # real install hit -- propagates out of `gather`, past `store.save_cursors()`, and out
    # of `watch`, which has nothing above it: the daemon stops on one optional scope. A
    # deliberate stop is the exception, and still is one.
    #
    # The failure is reported on the collection's own row. A row of its own would say it
    # nowhere: both renderers walk the names they were given, and a name outside that list
    # is simply not drawn -- which is the silent failure this whole guard exists to avoid.
    for name, outcome in zip(named, await gather(*named.values(), return_exceptions=True), strict=True):
        if isinstance(outcome, SyncAbortedError | KeyboardInterrupt):
            raise outcome
        if isinstance(outcome, BaseException):
            p.set(name, state="error", note=cli.oneline(f"{type(outcome).__name__}: {outcome}", 70))
    store.save_cursors()
    return store
