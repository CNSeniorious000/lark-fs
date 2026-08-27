"""Entity syncers. Each is an async function that pulls one collection into the store.

Incrementality: every collection stores a cursor (usually the newest timestamp
seen last run) in .lark-fs/cursors.json, and only asks Lark for what came after.
Search-backed collections (docs/minutes/meetings) have no server-side cursor, so
they re-list metadata (cheap) but skip fetching bodies for entities already on disk.
"""

from asyncio import create_task, gather
from collections.abc import MutableMapping
from datetime import UTC, datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from re import compile
from typing import Any

from reactivity import reactive

from . import cli
from .attachments import sync_attachments
from .cli import Aborted
from .store import Store

TZ = "+08:00"


FIRST_MONTH = "2023-01-01"  # only used the first time, before the store can answer


def _earliest(store: Store, collection: str, since: str) -> datetime:
    """Where the month walk should start.

    Scanning from a fixed year re-queries every empty month before the account had any
    data -- for a workspace that started this year that is most of the requests. Once
    anything is on disk it says where history actually begins; back up one month so an
    entry that lands late still gets picked up.
    """
    if since:
        return datetime.fromisoformat(since).replace(tzinfo=UTC)
    months = [m[1].replace(".", "-") for f in (store.root / collection).glob("*/*.yaml") if (m := RE_START.search(f.read_text()))]
    if not months:
        return datetime.fromisoformat(FIRST_MONTH).replace(tzinfo=UTC)
    return (datetime.fromisoformat(f"{min(months)}-01").replace(tzinfo=UTC) - timedelta(days=1)).replace(day=1)


def _months_between(start: datetime, end: datetime) -> int:
    return (end.year - start.year) * 12 + end.month - start.month + 1


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
    users: set[str] = set()
    total = 0

    async def drain(chat_id: str, start: str, end: str = "", limit: int = 0) -> tuple[str, bool]:
        """Walk one time window of a chat. Returns the newest create_time seen, and whether `limit` cut it short."""
        nonlocal total
        argv = ["im", "+chat-messages-list", "--chat-id", chat_id, "--order", "asc", "--no-reactions", *(["--start", start] if start else []), *(["--end", end] if end else [])]
        newest, seen = "", 0
        async for msg in cli.paginate(*argv, key="messages", prefetch=True):
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
            # store one second past the last message, or every run re-reads it
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
                _record_sender(store, r, users)
            cut.pop(item[0], None)
        p.set("messages", note=f"{total} messages, {len(cut)} truncated threads left")

    # A chat listing inlines at most 50 replies per thread; only this list remembers which
    # threads owe more, because the sweep will not pass them again -- the cursor moved on.
    if cut := store.cursors.get("threads_incomplete") or {}:
        await cli.spread(repair, sorted(cut.items()))
        store.save_cursors()

    async def rewrite(chunk: list[tuple[str, str]]):
        for msg in await repair_unreadable(store, chunk):
            media.extend(_index_media(msg))
            _record_sender(store, msg, users)
        for mid, _ in chunk:
            broken.pop(mid, None)  # a message the server no longer has is not coming back either
        p.set("messages", note=f"{total} messages, {len(broken)} unreadable files left")

    # The file is on disk but unparseable, so only the API still holds the real content.
    # `reindex` is what finds them -- a scan of every message is far too slow to run here.
    if broken := store.cursors.get("messages_unreadable") or {}:
        items = sorted(broken.items())
        await cli.spread(rewrite, [items[i : i + 50] for i in range(0, len(items), 50)])
        store.save_cursors()

    _flush_media(store, media)
    p.set("messages", state="done", note=f"{total} messages across {len(known)} chats")
    return known


async def _list_chats(store: Store) -> set[str]:
    """Every chat the user belongs to. Falls back to whatever is already mirrored."""
    found: set[str] = set()
    try:
        async for chat in cli.paginate("im", "+chat-list", "--types", "p2p,group", key="chats"):
            if cid := chat.get("chat_id"):
                found.add(cid)
                store.write_yaml(f"chats/{cid}/meta.yaml", _clean(chat))
    except cli.LarkError:
        pass
    return found or {d.name for d in (store.root / "chats").glob("oc_*")}


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
    store.write_yaml(f"{at}/meta.yaml", _thread_meta(root, replies))
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
        replies = [m async for m in cli.paginate("im", "+threads-messages-list", "--thread", thread, "--no-reactions", key="messages") if m.get("message_id")]
    except cli.LarkError:
        return []
    for r in replies:
        store.write_yaml(f"{at}/{r['message_id']}.yaml", _clean(r))
    meta = store.read_yaml(f"{at}/meta.yaml")
    store.write_yaml(f"{at}/meta.yaml", {**meta, "replies": len(replies), "has_more": False, "last_reply": max((r.get("create_time") or "" for r in replies), default=meta.get("last_reply", ""))})
    return replies


async def repair_unreadable(store: Store, chunk: list[tuple[str, str]]) -> list[dict]:
    """Re-fetch messages whose file on disk cannot be parsed, and write them again.

    A file written by a serializer that could not round-trip its content is on disk and
    says nothing readable, so the API is the only place the message still exists. Takes
    (message_id, relative path) pairs, at most 50 -- what `+messages-mget` accepts.

    Returns what came back, for the caller to index. A message the server no longer has
    simply does not come back, and no amount of retrying will change that.
    """
    try:
        data = await cli.run("im", "+messages-mget", "--message-ids", ",".join(mid for mid, _ in chunk), "--no-reactions")
    except cli.LarkError:
        return []
    returned = {m["message_id"]: m for m in (data or {}).get("messages") or []}
    written = []
    for mid, rel in chunk:
        if msg := returned.get(mid):
            store.write_yaml(rel, _clean(msg))
            written.append(msg)
    return written


def _index_media(msg: dict) -> list[dict]:
    """Collect media references as keys/URLs. Bytes are never downloaded."""
    rows = [
        {
            "key": img or md or key,
            "name": name or "",
            "msg_type": msg.get("msg_type", ""),
            "message_id": msg.get("message_id"),
            "chat_id": msg.get("chat_id"),
            "create_time": msg.get("create_time", ""),
        }
        # a message body is not always a string: one real message is a 48-digit number, and
        # `cli.oneline` already coerces for the same reason
        for img, md, key, name in RE_MEDIA.findall(str(msg.get("content") or ""))
    ]
    return rows


async def sync_chat_meta(store: Store, p: Progress, chat_ids: set[str]):
    """Chat rosters. Metadata already landed when messages listed the chats.

    Membership is the part only this pass can get, and it is the expensive part, so a
    chat whose roster is already on disk is skipped entirely.
    """
    p.set("chats", state="running")
    known = chat_ids or {d.name for d in (store.root / "chats").glob("oc_*")}
    todo = [c for c in known if not store.exists(f"chats/{c}/members.yaml")]
    p.set("chats", total=len(todo), note=f"{len(known)} chats")
    if not todo:
        p.set("chats", state="done", note=f"{len(known)} chats, rosters up to date")
        return

    async def one(chat_id: str):
        try:
            members = await cli.run("im", "+chat-members-list", "--chat-id", chat_id, "--page-all")
        except cli.LarkError:
            p.bump("chats")  # p2p chats and ones we lack scope for simply have no roster
            return
        store.write_yaml(f"chats/{chat_id}/members.yaml", _clean(members))
        # bots and users share one directory, and only 67 of 81 bots seen carry app_id -- without
        # this flag the other 14 are indistinguishable from people once on disk
        people = ((members or {}).get("users") or []) + [{**b, "is_bot": True} for b in ((members or {}).get("bots") or [])]
        for u in people:
            if oid := u.get("member_id") or u.get("open_id"):
                store.write_yaml(f"users/{oid}/meta.yaml", _clean({"open_id": oid, **u}))
        p.bump("chats", last=f"{len(people)} members")

    await cli.spread(one, todo)
    p.set("chats", state="done", note=f"{len(known)} chats")


# Everything a roster carries about a person beyond their id. `localized_name` is the one
# that matters: it holds the parenthesised alias that tells two same-named colleagues apart.
PROFILE_FIELDS = ("localized_name", "enterprise_email", "email", "department", "p2p_chat_id", "is_activated")
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
    tenant = (((await cli.run("contact", "+get-user")) or {}).get("user") or {}).get("tenant_key")
    on_disk = {d.name: store.read_yaml(f"users/{d.name}/meta.yaml") for d in (store.root / "users").glob("ou_*")}
    todo = [oid for oid, u in on_disk.items() if u.get("tenant_key") == tenant and "localized_name" not in u]
    p.set("profiles", total=len(todo), note=f"{len(on_disk)} known")
    if not todo:
        p.set("profiles", state="done", note="up to date")
        return

    resolved = 0

    async def one(batch: list[str]):
        nonlocal resolved
        d = await cli.run("contact", "+search-user", "--user-ids", ",".join(batch), "--as", "user") or {}
        rows = d.get("users") or []
        for row in rows:
            rel = f"users/{row['open_id']}/meta.yaml"
            # a roster row is the richer one for name/member_id; only the profile-only fields are taken
            store.write_yaml(rel, _clean({**store.read_yaml(rel), **{k: row[k] for k in PROFILE_FIELDS if k in row}}))
        resolved += len(rows)
        p.bump("profiles", len(batch))

    await cli.spread(one, [todo[i : i + BATCH] for i in range(0, len(todo), BATCH)])
    # the shortfall is deactivated accounts and bots, which never resolve and so are asked
    # for on every run -- cheap enough at one request per 19, and they come back if rehired
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


async def sync_docs(store: Store, p: Progress, *, queries: list[str] | None = None):
    """Cloud docs discovered via search and via wiki nodes, exported to markdown, plus comments."""
    p.set("docs", state="running")
    seen: dict[str, dict] = {}
    alias = _wiki_aliases(store)
    for q in queries or DOC_QUERIES:
        try:
            async for r in cli.paginate("drive", "+search", "--query", q, key="results"):
                meta = r.get("result_meta") or {}
                if not (token := str(meta.get("token") or "")):
                    continue
                if r.get("entity_type") == "WIKI":
                    token = alias.get(token, token)  # the node token this hit carries is not the document's own
                if token in seen:
                    continue
                title = _clean(r.get("title_highlighted"))
                _note_tenant(meta.get("url", ""))
                seen[token] = {**meta, "token": token, "title": title}
                store.write_yaml(f"docs/{token}/meta.yaml", {**meta, "token": token, "entity_type": r.get("entity_type"), "title": title})
                p.bump("docs", last=cli.oneline(title))
        except cli.LarkError:
            continue

    # wiki nodes point at real documents and are enumerated exhaustively, unlike search
    for node in store.glob_rows("wiki/*/nodes.yaml"):
        if (token := node.get("obj_token")) and node.get("obj_type") in ("docx", "doc", "sheet", "bitable") and token not in seen:
            seen[token] = {"title": node.get("title", "")}
            store.write_yaml(
                f"docs/{token}/meta.yaml",
                {"token": token, "title": node.get("title", ""), "obj_type": node.get("obj_type"), "wiki_node_token": node.get("node_token", ""), "space_id": node.get("space_id", "")},
            )
            p.bump("docs", last=cli.oneline(node.get("title")))

    # bodies are the expensive part: fetch one only if we have none, or if the server's
    # update_time moved past the copy we already wrote. A doc can be edited at any time,
    # so there is no window to bound this -- the timestamp is the only reliable signal.
    todo = [t for t, meta in seen.items() if _doc_is_stale(store, t, meta)]
    p.set("docs", done=0, total=len(todo), note=f"{len(seen)} docs")

    async def body(token: str):
        title = cli.oneline(seen[token].get("title") or token, 48)
        try:
            data = await cli.run("docs", "+fetch", "--doc", token, "--doc-format", "markdown", subject=f"fetch {title}")
        except cli.LarkError:
            return  # transient: leave it due, the next run retries it
        finally:
            p.bump("docs", last=title)  # count attempts, not successes, or the fraction never reaches its end
        # sheets, bitables and the like answer fine but carry no markdown body; record that,
        # or every run retries the same few hundred tokens forever
        content = ((data or {}).get("document") or {}).get("content")
        store.write(f"docs/{token}/content.md", content) if content else store.write(f"docs/{token}/.nobody", "")
        if store.exists(f"docs/{token}/.nocomments"):
            return
        try:
            comments = await cli.run("drive", "+list-comments", "--token", token, "--type", "docx", "--solved-status", "all")
            if comments:
                store.write_yaml(f"docs/{token}/comments.yaml", comments)
        except cli.LarkError as e:
            # a token Drive does not recognise never will; anything else may just be a
            # rate limit, and marking that would lose the comments for good
            if e.is_missing:
                store.write(f"docs/{token}/.nocomments", "")

    await cli.spread(body, todo)
    p.set("docs", state="done", note=f"{len(seen)} docs")


def _doc_is_stale(store: Store, token: str, meta: dict) -> bool:
    """True when the body is missing, or the server copy is newer than ours."""
    body = store.root / f"docs/{token}/content.md"
    if not body.exists():
        return not (store.root / f"docs/{token}/.nobody").exists()
    remote = meta.get("update_time")
    return bool(remote and remote > body.stat().st_mtime)


def _edit_signal(msg: dict) -> tuple:
    """What actually changes when a message is edited."""
    return (msg.get("update_time") or "", str(msg.get("content") or ""), bool(msg.get("updated")))


async def recheck_messages(store: Store, p: Progress, *, window_days: int = 30, batch: int = 50):
    """Re-verify recently synced messages, since edits and recalls leave no forward trace.

    `+messages-search` cannot filter by update time and never returns recalled messages,
    but `+messages-mget` reports `update_time` per id. So we replay the ids we already
    have from the last `window_days` and rewrite only what actually changed -- one request
    per 50 messages, which is far cheaper than re-fetching the window.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=window_days)).strftime("%Y-%m")
    recent = sorted((f for f in (store.root / "chats").glob("*/messages/*/*.yaml") if f.parent.name >= cutoff), key=lambda f: f.parent.name, reverse=True)
    if not recent:
        return
    p.set("recheck", state="running", total=len(recent))

    by_id = {f.stem: f for f in recent}
    ids = list(by_id)
    changed = 0
    recalled: list[dict] = []
    for i in range(0, len(ids), batch):
        chunk = ids[i : i + batch]
        try:
            data = await cli.run("im", "+messages-mget", "--message-ids", ",".join(chunk), "--no-reactions")
        except cli.LarkError:
            p.bump("recheck", len(chunk))
            continue
        returned = {m["message_id"]: m for m in (data or {}).get("messages") or []}
        for mid in chunk:
            if not (msg := returned.get(mid)):
                # recalled: it is gone from the server, but the local copy is the only
                # record left, so mark it rather than delete it
                recalled.append({"message_id": mid, "path": str(by_id[mid].relative_to(store.root))})
                continue
            # mget returns a wider field set than search, so comparing whole payloads
            # would rewrite every file; the edit signal is update_time plus the body
            rel = str(by_id[mid].relative_to(store.root))
            before = store.read_yaml(rel)
            if _edit_signal(msg) != _edit_signal(before):
                merged = {**before, **_clean(msg)}
                store.write_yaml(rel, merged)
                changed += 1
        p.bump("recheck", len(chunk), last=f"{changed} changed, {len(recalled)} recalled")
    if recalled:
        store.write_yaml("recalled.yaml", recalled)
    p.set("recheck", state="done", note=f"{changed} updated, {len(recalled)} recalled")


def _rows(payload: dict) -> list[dict]:
    """+record-list returns a column matrix; zip it back into named rows keyed by record_id."""
    fields, data, ids = payload.get("fields") or [], payload.get("data") or [], payload.get("record_id_list") or []
    return [{"record_id": rid, **dict(zip(fields, row, strict=False))} for rid, row in zip(ids, data, strict=False)]


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


async def sync_minutes(store: Store, p: Progress, *, since: str = ""):
    """Minutes (妙记): metadata, AI summary, chapters, todos, and full transcript.

    `+search` caps at 50 results per query no matter the filters, so a single call over
    the whole history silently reports 50. Walking month by month lifts the ceiling.
    """
    p.set("minutes", state="running")
    found: dict[str, dict] = {}
    month = _earliest(store, "minutes", since)
    now = datetime.now(UTC)
    p.set("minutes", total=_months_between(month, now))
    scanned = 0
    while month < now:
        nxt = (month + timedelta(days=32)).replace(day=1)
        try:
            async for item in cli.paginate("minutes", "+search", "--start", month.strftime("%Y-%m-%d"), "--end", nxt.strftime("%Y-%m-%d"), key="items"):
                if token := item.get("token"):
                    found[token] = item
        except cli.LarkError as e:
            if not e.is_pagination_exhausted:
                p.set("minutes", state="error", note=_reason(e))
                return
        scanned += 1
        p.set("minutes", done=scanned, note=f"scanning {month:%Y-%m} · {len(found)} found")
        month = nxt

    for token, item in found.items():
        store.write_yaml(f"minutes/{token}/meta.yaml", _clean(item))

    todo = [t for t in found if not store.exists(f"minutes/{t}/transcript.txt")]
    p.set("minutes", done=0, total=len(todo), note=f"fetching transcripts · {len(found)} minutes")

    async def detail(token: str):
        Aborted.check()
        try:
            # +detail writes transcript.txt into <cwd>/minutes/<token>/ itself -- run it in the store so it lands in place
            data = await cli.run("minutes", "+detail", "--minute-tokens", token, "--transcript", "--summary", "--todo", "--chapter", "--keyword", cwd=str(store.root))
        except cli.LarkError:
            return
        finally:
            p.bump("minutes", last=cli.summarise_title((found.get(token) or {}).get("display_info")))
        for m in (data or {}).get("minutes") or []:
            arts = m.get("artifacts") or {}
            store.write_yaml(f"minutes/{token}/detail.yaml", _clean({k: v for k, v in m.items() if k != "artifacts"}))
            if chapters := arts.get("chapters"):
                store.write_yaml(f"minutes/{token}/chapters.yaml", _clean(chapters))
            if todos := arts.get("todos"):
                store.write_yaml(f"minutes/{token}/todos.yaml", _clean(todos))
            if summary := arts.get("summary"):
                store.write(f"minutes/{token}/summary.md", summary if isinstance(summary, str) else str(summary))

    await cli.spread(detail, todo)
    p.set("minutes", state="done", note=f"{len(found)} minutes")


async def sync_meetings(store: Store, p: Progress, *, since: str = ""):
    """VC meeting records; links each meeting to its minute_token and note_id.

    Like minutes, `+search` has a per-query ceiling (150 here), so walk month by month.
    """
    p.set("meetings", state="running")
    ids: list[str] = []
    month = _earliest(store, "meetings", since)
    now = datetime.now(UTC)
    p.set("meetings", total=_months_between(month, now))
    scanned = 0
    while month < now:
        nxt = (month + timedelta(days=32)).replace(day=1)
        try:
            async for item in cli.paginate("vc", "+search", "--start", month.strftime("%Y-%m-%d"), "--end", nxt.strftime("%Y-%m-%d"), key="items"):
                if mid := item.get("id"):
                    ids.append(mid)
                    store.write_yaml(f"meetings/{mid}/meta.yaml", _clean(item))
        except cli.LarkError as e:
            if not e.is_pagination_exhausted:
                p.set("meetings", state="error", note=_reason(e))
                return
        scanned += 1
        p.set("meetings", done=scanned, note=f"scanning {month:%Y-%m} · {len(ids)} found")
        month = nxt

    todo = [i for i in ids if not store.exists(f"meetings/{i}/detail.yaml")]
    p.set("meetings", done=0, total=len(todo), note=f"fetching details · {len(ids)} meetings")

    async def details(batch: list[str]):
        try:
            data = await cli.run("vc", "+detail", "--meeting-ids", ",".join(batch))
        except cli.LarkError:
            return
        finally:
            p.bump("meetings", len(batch))
        for m in (data or {}).get("meetings") or []:
            store.write_yaml(f"meetings/{m['meeting_id']}/detail.yaml", _clean(m))

    await cli.spread(details, [todo[i : i + 20] for i in range(0, len(todo), 20)])
    p.set("meetings", state="done", note=f"{len(ids)} meetings")


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

    p.set("bases", total=len(tokens))

    async def one(app_token: str):
        try:
            tables = await cli.run("base", "+table-list", "--base-token", app_token)
        except cli.LarkError:
            return
        for t in (tables or {}).get("tables") or []:
            tid = t.get("id")
            store.write_yaml(f"bases/{app_token}/tables/{tid}/meta.yaml", t)
            rel = f"bases/{app_token}/tables/{tid}/records.yaml"
            if store.exists(rel):
                continue  # records are re-pulled only on demand; full-table diffing is a later concern
            # +record-list is offset-based (not page-token) and defaults to a markdown table
            rows: list[dict] = []
            while True:
                try:
                    recs = await cli.run("base", "+record-list", "--base-token", app_token, "--table-id", tid, "--limit", "200", "--offset", str(len(rows)))
                except cli.LarkError:
                    break
                rows += _rows(recs or {})
                if not (recs or {}).get("has_more"):
                    break
            store.write_yaml(rel, _clean(rows))
        p.bump("bases")

    await cli.spread(one, tokens)
    p.set("bases", state="done")


async def sync_wiki(store: Store, p: Progress):
    p.set("wiki", state="running")
    try:
        spaces = await cli.run("wiki", "+space-list", "--page-all")
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
        argv = ["wiki", "+node-list", "--space-id", sid, "--page-all", *(["--parent-node-token", parent] if parent else [])]
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

    async def one(space: dict):
        if not (sid := space.get("space_id")):
            return
        store.write_yaml(f"wiki/{sid}/meta.yaml", space)
        nodes, whole = await walk(sid)
        rel = f"wiki/{sid}/nodes.yaml"
        # A partial walk must not overwrite a complete one. This node list is the only
        # place a wiki node_token can be resolved to the document it points at, and one
        # rate-limited level used to be enough to replace the whole thing with `[]`.
        if whole or not store.exists(rel):
            store.write_yaml(rel, _clean(nodes))
        p.set("wiki", note=f"{len(nodes)} nodes" + ("" if whole else ", partial -- kept the copy on disk"))

    await cli.spread(one, items)
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
    want = set(only or ALL)
    tasks = []

    # The roster is one request, and it is all the chat pass ever wanted. It used to be
    # taken from the message sweep's return value -- which meant waiting out a sweep of
    # every message in every chat for a set that sync_messages computes on its first line.
    roster = create_task(_list_chats(store)) if want & {"messages", "chats"} else None

    async def walk_messages():
        return await sync_messages(store, p, chat_ids=await roster if roster else None)

    messages = create_task(walk_messages()) if "messages" in want else None
    wiki = create_task(sync_wiki(store, p)) if "wiki" in want else None

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
        await sync_docs(store, p)

    async def files():
        # the media index is a by-product of the message sweep; on its own it reads
        # whatever is already on disk, which is what `--only files` is for
        if messages:
            await messages
        await sync_attachments(store, p)

    if "profiles" in want:
        tasks.append(profiles())
    if "docs" in want:
        tasks.append(docs())
    if "files" in want:
        tasks.append(files())
    if "minutes" in want:
        tasks.append(sync_minutes(store, p))
    if "meetings" in want:
        tasks.append(sync_meetings(store, p))
    if "bases" in want:
        tasks.append(sync_bases(store, p))

    # A producer is also awaited here. A task can be awaited more than once, so this costs
    # nothing and removes the need to keep "who else awaits this" in step with the graph --
    # get that wrong and the task is simply abandoned when gather returns.
    tasks += [t for t in (roster, messages, wiki, rosters) if t]

    await gather(*tasks)
    store.save_cursors()
    return store
