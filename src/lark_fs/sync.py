"""Entity syncers. Each is an async function that pulls one collection into the store.

Incrementality: every collection stores a cursor (usually the newest timestamp
seen last run) in .lark-fs/cursors.json, and only asks Lark for what came after.
Search-backed collections (docs/minutes/meetings) have no server-side cursor, so
they re-list metadata (cheap) but skip fetching bodies for entities already on disk.
"""

from asyncio import gather
from datetime import UTC, datetime, timedelta
from html import unescape
from pathlib import Path
from re import compile

from reactivity import reactive

from . import cli
from .store import Store

TZ = "+08:00"


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + TZ


class Aborted:
    """Set when the user asks to stop; long walks check it between units of work."""

    flag = False

    @classmethod
    def check(cls):
        if cls.flag:
            raise SyncAbortedError


class SyncAbortedError(Exception):
    """Cooperative stop: raised at a slice boundary so the cursor stays consistent."""


class Progress:
    """Sync state as a reactive store; readers re-run automatically when a row changes.

    Rows are replaced wholesale rather than mutated in place -- the reactive mapping
    tracks per-key identity, so an in-place dict update would not notify subscribers.
    """

    def __init__(self):
        self.rows = reactive({})

    def _row(self, name: str) -> dict:
        return self.rows.get(name) or {"name": name, "state": "pending", "done": 0, "total": None, "note": ""}

    def set(self, name: str, **fields):
        self.rows[name] = {**self._row(name), **fields}

    def bump(self, name: str, n: int = 1):
        row = self._row(name)
        self.rows[name] = {**row, "state": "running", "done": row["done"] + n}


async def sync_messages(store: Store, p: Progress, *, window_days: int = 30, slice_hours: int = 12):
    """Messages via the global search endpoint, walked forward in time slices.

    Enumerating chats needs im:chat:read, which may be unauthorized; search only
    needs the search scope and covers every chat the user can see, so it is the
    primary path. Each message lands under its chat, partitioned by month.

    Two constraints shape this loop. `--page-all` caps at 40 pages (800 messages),
    so a wide window silently truncates; and the endpoint rate-limits hard enough
    that a run *will* be cut short. So we advance in small slices and commit the
    cursor after each one -- an interrupted run resumes where it stopped instead
    of discarding everything it already wrote.
    """
    p.set("messages", state="running")
    cursor = store.cursors.get("messages")
    start = datetime.fromisoformat(cursor) if cursor else datetime.now(UTC) - timedelta(days=window_days)
    end = datetime.now(UTC)
    seen_chats: set[str] = set()
    media: list[dict] = []
    known_users: set[str] = set()

    while start < end:
        Aborted.check()
        stop = min(start + timedelta(hours=slice_hours), end)
        argv = ["im", "+messages-search", "--query", "", "--start", _iso(start), "--end", _iso(stop), "--page-all", "--no-reactions"]
        try:
            async for msg in cli.paginate(*argv, key="messages"):
                chat_id, mid = msg.get("chat_id"), msg.get("message_id")
                if not chat_id or not mid:
                    continue
                seen_chats.add(chat_id)
                created = msg.get("create_time", "")
                month = created[:7] or "unknown"
                thread = msg.get("thread_id")
                rel = f"chats/{chat_id}/threads/{thread}/{mid}.yaml" if thread else f"chats/{chat_id}/messages/{month}/{mid}.yaml"
                store.write_yaml(rel, _clean(msg))
                media += _index_media(msg)
                _record_sender(store, msg, known_users)
                p.bump("messages")
        except cli.LarkError as e:
            # keep whatever this run already committed; the next run picks up from `start`
            _flush_media(store, media)
            store.cursors["messages"] = start.isoformat()
            store.save_cursors()
            p.set("messages", state="error", note=f"stopped at {start:%m-%d %H:%M}: {_reason(e)}")
            return seen_chats

        _flush_media(store, media)
        media.clear()
        start = stop
        store.cursors["messages"] = start.isoformat()
        store.save_cursors()
        p.set("messages", note=f"through {start:%m-%d %H:%M}")

    p.set("messages", state="done", note=f"{len(seen_chats)} chats")
    return seen_chats


def _reason(e: cli.LarkError) -> str:
    return e.payload.get("error", {}).get("message", "failed") if isinstance(e.payload, dict) else "failed"


# media keys are embedded in the message body, not a separate field:
#   `[Image: img_v3_...]`  `![Image](img_v3_...)`  `<file key="file_v3_..." name="..."/>`
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


def _index_media(msg: dict) -> list[dict]:
    """Collect media references as keys/URLs. Bytes are never downloaded."""
    rows = [
        {"key": img or md or key, "name": name or "", "msg_type": msg.get("msg_type", ""), "message_id": msg.get("message_id"), "chat_id": msg.get("chat_id"), "create_time": msg.get("create_time", "")}
        for img, md, key, name in RE_MEDIA.findall(msg.get("content") or "")
    ]
    return rows


async def sync_chat_meta(store: Store, p: Progress, chat_ids: set[str]):
    """Chat metadata + membership for every chat the user belongs to.

    `+chat-list` covers chats that have said nothing in the synced window, so it finds
    more than the message sweep does; ids seen in messages are unioned in on top.
    """
    p.set("chats", state="running")
    listed: dict[str, dict] = {}
    try:
        async for chat in cli.paginate("im", "+chat-list", "--types", "p2p,group", key="chats"):
            if cid := chat.get("chat_id"):
                listed[cid] = chat
    except cli.LarkError:
        pass  # without im:chat:read we still have whatever the messages revealed

    todo = [c for c in listed.keys() | chat_ids if not store.exists(f"chats/{c}/members.yaml")]
    p.set("chats", total=len(todo))
    if not todo:
        p.set("chats", state="done", note=f"{len(listed)} known, up to date")
        return

    async def one(chat_id: str):
        try:
            meta = await cli.run("im", "chats", "get", "--chat-id", chat_id)
            store.write_yaml(f"chats/{chat_id}/meta.yaml", _clean(meta))
        except cli.LarkError:
            if base := listed.get(chat_id):
                store.write_yaml(f"chats/{chat_id}/meta.yaml", _clean(base))
        try:
            members = await cli.run("im", "+chat-members-list", "--chat-id", chat_id, "--page-all")
        except cli.LarkError:
            p.bump("chats")
            return
        store.write_yaml(f"chats/{chat_id}/members.yaml", _clean(members))
        for u in ((members or {}).get("users") or []) + ((members or {}).get("bots") or []):
            if oid := u.get("member_id") or u.get("open_id"):
                store.write_yaml(f"users/{oid}/meta.yaml", _clean({"open_id": oid, **u}))
        p.bump("chats")

    await gather(*(one(c) for c in todo))
    p.set("chats", state="done", note=f"{len(listed)} listed")


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


async def sync_docs(store: Store, p: Progress, *, queries: list[str]):
    """Cloud docs discovered via search, exported to markdown, plus comments."""
    p.set("docs", state="running")
    seen: set[str] = set()
    for q in queries:
        try:
            async for r in cli.paginate("drive", "+search", "--query", q, key="results"):
                meta = r.get("result_meta") or {}
                token = meta.get("token")
                if not token or token in seen:
                    continue
                seen.add(token)
                store.write_yaml(f"docs/{token}/meta.yaml", {**meta, "entity_type": r.get("entity_type"), "title": _clean(r.get("title_highlighted"))})
                p.bump("docs")
        except cli.LarkError:
            continue

    # bodies are the expensive part -- only fetch ones missing from disk
    todo = [t for t in seen if not store.exists(f"docs/{t}/content.md")]
    p.set("docs", total=len(seen), note=f"{len(todo)} bodies to fetch")

    async def body(token: str):
        try:
            data = await cli.run("docs", "+fetch", "--doc", token, "--doc-format", "markdown")
            if content := ((data or {}).get("document") or {}).get("content"):
                store.write(f"docs/{token}/content.md", content)
        except cli.LarkError:
            pass
        try:
            comments = await cli.run("drive", "+list-comments", "--token", token, "--type", "docx", "--solved-status", "all")
            if comments:
                store.write_yaml(f"docs/{token}/comments.yaml", comments)
        except cli.LarkError:
            pass

    await gather(*(body(t) for t in todo))
    p.set("docs", state="done")


def _rows(payload: dict) -> list[dict]:
    """+record-list returns a column matrix; zip it back into named rows keyed by record_id."""
    fields, data, ids = payload.get("fields") or [], payload.get("data") or [], payload.get("record_id_list") or []
    return [{"record_id": rid, **dict(zip(fields, row, strict=False))} for rid, row in zip(ids, data, strict=False)]


def _clean(value):
    """Lark's search endpoints return HTML-entity-escaped text with <h> hit markers.
    Left as-is those become `&lt;b&gt;` on disk, which breaks plain-text grep."""
    if isinstance(value, str):
        return unescape(value.replace("<h>", "").replace("</h>", ""))
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


async def sync_minutes(store: Store, p: Progress, *, since: str = "2020-01-01"):
    """Minutes (妙记): metadata, AI summary, chapters, todos, and full transcript."""
    p.set("minutes", state="running")
    tokens: list[dict] = []
    try:
        async for item in cli.paginate("minutes", "+search", "--start", since, key="items"):
            tokens.append(item)
    except cli.LarkError as e:
        p.set("minutes", state="error", note=str(e.payload)[:60])
        return

    p.set("minutes", total=len(tokens))
    for item in tokens:
        token = item.get("token")
        if not token:
            continue
        store.write_yaml(f"minutes/{token}/meta.yaml", _clean(item))
        p.bump("minutes")

    todo = [t["token"] for t in tokens if t.get("token") and not store.exists(f"minutes/{t['token']}/transcript.txt")]
    p.set("minutes", note=f"{len(todo)} transcripts to fetch")

    async def detail(token: str):
        try:
            # +detail writes transcript.txt into <cwd>/minutes/<token>/ itself -- run it in the store so it lands in place
            data = await cli.run("minutes", "+detail", "--minute-tokens", token, "--transcript", "--summary", "--todo", "--chapter", "--keyword", cwd=str(store.root))
        except cli.LarkError:
            return
        for m in (data or {}).get("minutes") or []:
            arts = m.get("artifacts") or {}
            store.write_yaml(f"minutes/{token}/detail.yaml", {k: v for k, v in m.items() if k != "artifacts"})
            if chapters := arts.get("chapters"):
                store.write_yaml(f"minutes/{token}/chapters.yaml", chapters)
            if todos := arts.get("todos"):
                store.write_yaml(f"minutes/{token}/todos.yaml", todos)
            if summary := arts.get("summary"):
                store.write(f"minutes/{token}/summary.md", summary if isinstance(summary, str) else str(summary))

    await gather(*(detail(t) for t in todo))
    p.set("minutes", state="done")


async def sync_meetings(store: Store, p: Progress, *, since_days: int = 365):
    """VC meeting records; links each meeting to its minute_token and note_id."""
    p.set("meetings", state="running")
    start = (datetime.now(UTC) - timedelta(days=since_days)).strftime("%Y-%m-%d")
    ids: list[str] = []
    try:
        async for item in cli.paginate("vc", "+search", "--start", start, key="items"):
            mid = item.get("id")
            if not mid:
                continue
            ids.append(mid)
            store.write_yaml(f"meetings/{mid}/meta.yaml", _clean(item))
            p.bump("meetings")
    except cli.LarkError as e:
        p.set("meetings", state="error", note=str(e.payload)[:60])
        return

    todo = [i for i in ids if not store.exists(f"meetings/{i}/detail.yaml")]
    for batch in [todo[i : i + 20] for i in range(0, len(todo), 20)]:
        try:
            data = await cli.run("vc", "+detail", "--meeting-ids", ",".join(batch))
        except cli.LarkError:
            continue
        for m in (data or {}).get("meetings") or []:
            store.write_yaml(f"meetings/{m['meeting_id']}/detail.yaml", m)
    p.set("meetings", state="done")


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

    await gather(*(one(t) for t in tokens))
    p.set("bases", state="done")


async def sync_wiki(store: Store, p: Progress):
    p.set("wiki", state="running")
    try:
        spaces = await cli.run("wiki", "+space-list", "--page-all")
    except cli.LarkError as e:
        p.set("wiki", state="error", note=str(e.payload)[:60])
        return
    items = (spaces or {}).get("spaces") or []
    p.set("wiki", total=len(items))

    async def walk(sid: str, parent: str | None = None) -> list[dict]:
        """Wiki nodes form a tree; +node-list only returns one level, so recurse into has_child."""
        argv = ["wiki", "+node-list", "--space-id", sid, "--page-all", *(["--parent-node-token", parent] if parent else [])]
        try:
            data = await cli.run(*argv)
        except cli.LarkError:
            return []
        nodes = (data or {}).get("nodes") or []
        children = await gather(*(walk(sid, n["node_token"]) for n in nodes if n.get("has_child")))
        return nodes + [n for group in children for n in group]

    async def one(space: dict):
        sid = space.get("space_id")
        store.write_yaml(f"wiki/{sid}/meta.yaml", space)
        store.write_yaml(f"wiki/{sid}/nodes.yaml", await walk(sid))
        p.bump("wiki")

    await gather(*(one(s) for s in items))
    p.set("wiki", state="done")


ALL = ["messages", "chats", "docs", "minutes", "meetings", "bases", "wiki"]


async def sync_all(root: Path, p: Progress, only: list[str] | None = None):
    store = Store(root)
    want = set(only or ALL)
    chat_ids: set[str] = set()
    if "messages" in want:
        # runs first so chats can pick up ids from conversations `+chat-list` misses,
        # but chats no longer depends on it succeeding -- a rate-limited message sweep
        # must not take the roster down with it
        chat_ids = await sync_messages(store, p) or set()
    tasks = []
    if "chats" in want:
        tasks.append(sync_chat_meta(store, p, chat_ids))
    if "docs" in want:
        tasks.append(sync_docs(store, p, queries=[""]))
    if "minutes" in want:
        tasks.append(sync_minutes(store, p))
    if "meetings" in want:
        tasks.append(sync_meetings(store, p))
    if "bases" in want:
        tasks.append(sync_bases(store, p))
    if "wiki" in want:
        tasks.append(sync_wiki(store, p))
    await gather(*tasks)
    store.save_cursors()
    return store
