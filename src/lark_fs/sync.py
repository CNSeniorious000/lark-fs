"""Entity syncers. Each is an async function that pulls one collection into the store.

Incrementality: every collection stores a cursor (usually the newest timestamp
seen last run) in .lark-fs/cursors.json, and only asks Lark for what came after.
Search-backed collections (docs/minutes/meetings) have no server-side cursor, so
they re-list metadata (cheap) but skip fetching bodies for entities already on disk.
"""

from asyncio import create_task, gather, sleep
from datetime import UTC, datetime, timedelta
from html import unescape
from pathlib import Path
from re import compile

from reactivity import reactive

from . import cli
from .cli import Aborted
from .store import Store

TZ = "+08:00"


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + TZ


class Progress:
    """Sync state as a reactive store; readers re-run automatically when a row changes.

    Rows are replaced wholesale rather than mutated in place -- the reactive mapping
    tracks per-key identity, so an in-place dict update would not notify subscribers.
    """

    def __init__(self):
        self.rows = reactive({})

    def _row(self, name: str) -> dict:
        return self.rows.get(name) or {"name": name, "state": "pending", "done": 0, "total": None, "note": "", "last": ""}

    def set(self, name: str, **fields):
        self.rows[name] = {**self._row(name), **fields}

    def bump(self, name: str, n: int = 1, last: str = ""):
        """`last` is what was just written -- a title or id -- shown so the run is legible."""
        row = self._row(name)
        self.rows[name] = {**row, "state": "running", "done": row["done"] + n, "last": last or row["last"]}


async def sync_messages(store: Store, p: Progress, *, window_days: int = 30, slice_hours: int = 12):
    """Messages via the global search endpoint, walked forward in time slices.

    Enumerating chats needs im:chat:read, which may be unauthorized; search only
    needs the search scope and covers every chat the user can see, so it is the
    primary path. Each message lands under its chat, partitioned by month.

    Two constraints shape this loop. The endpoint's own `--page-all` caps at 40 pages
    (800 messages) and returns them in one blocking call, so we page it ourselves --
    that lifts the cap and lets progress tick per page instead of jumping. And it
    rate-limits hard enough that a run *will* be cut short, so we advance in small
    slices and commit the cursor after each one: an interrupted run resumes where it
    stopped instead of discarding everything it already wrote.
    """
    cli.current_group.set("messages")
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
        p.set("messages", state="running", note=f"{start:%m-%d %H:%M}")
        argv = ["im", "+messages-search", "--query", "", "--start", _iso(start), "--end", _iso(stop), "--no-reactions"]
        try:
            async for msg in cli.paginate(*argv, key="messages", prefetch=True):
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
                sender = (msg.get("sender") or {}).get("name") or ""
                p.bump("messages", last=f"{msg.get('chat_name') or chat_id}  {sender}: {_oneline(msg.get('content'))}")
                await sleep(0)  # keep the UI responsive; repaint rate is the TUI's concern
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
    cli.current_group.set("chats")
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


# Drive search returns a ranked slice, not the corpus: an empty query yields far fewer
# hits than a common word does, so coverage comes from unioning several probes.
DOC_QUERIES = ["", "a", "e", "的", "会议", "设计", "项目", "需求", "方案", "数据", "模型", "文档", "记录", "计划"]


async def sync_docs(store: Store, p: Progress, *, queries: list[str] | None = None):
    """Cloud docs discovered via search and via wiki nodes, exported to markdown, plus comments."""
    cli.current_group.set("docs")
    p.set("docs", state="running")
    seen: dict[str, dict] = {}
    for q in queries or DOC_QUERIES:
        try:
            async for r in cli.paginate("drive", "+search", "--query", q, key="results"):
                meta = r.get("result_meta") or {}
                token = meta.get("token")
                if not token or token in seen:
                    continue
                seen[token] = meta
                title = _clean(r.get("title_highlighted"))
                store.write_yaml(f"docs/{token}/meta.yaml", {**meta, "entity_type": r.get("entity_type"), "title": title})
                p.bump("docs", last=_oneline(title))
        except cli.LarkError:
            continue

    # wiki nodes point at real documents and are enumerated exhaustively, unlike search
    for space in (store.root / "wiki").glob("*/nodes.yaml"):
        for node in store.read_yaml_rows(f"wiki/{space.parent.name}/nodes.yaml"):
            if (token := node.get("obj_token")) and node.get("obj_type") in ("docx", "doc", "sheet", "bitable") and token not in seen:
                seen[token] = {}
                store.write_yaml(f"docs/{token}/meta.yaml", {"token": token, "title": node.get("title", ""), "obj_type": node.get("obj_type"), "wiki_node_token": node.get("node_token", ""), "space_id": node.get("space_id", "")})
                p.bump("docs", last=_oneline(node.get("title")))

    # bodies are the expensive part: fetch one only if we have none, or if the server's
    # update_time moved past the copy we already wrote. A doc can be edited at any time,
    # so there is no window to bound this -- the timestamp is the only reliable signal.
    todo = [t for t, meta in seen.items() if _doc_is_stale(store, t, meta)]
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


def _doc_is_stale(store: Store, token: str, meta: dict) -> bool:
    """True when the body is missing, or the server copy is newer than ours."""
    body = store.root / f"docs/{token}/content.md"
    if not body.exists():
        return True
    remote = meta.get("update_time")
    return bool(remote and remote > body.stat().st_mtime)


def _edit_signal(msg: dict) -> tuple:
    """What actually changes when a message is edited."""
    return (msg.get("update_time") or "", msg.get("content") or "", bool(msg.get("updated")))


async def recheck_messages(store: Store, p: Progress, *, window_days: int = 30, batch: int = 50):
    """Re-verify recently synced messages, since edits and recalls leave no forward trace.

    `+messages-search` cannot filter by update time and never returns recalled messages,
    but `+messages-mget` reports `update_time` per id. So we replay the ids we already
    have from the last `window_days` and rewrite only what actually changed -- one request
    per 50 messages, which is far cheaper than re-fetching the window.
    """
    cli.current_group.set("messages")
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


def _oneline(text, limit: int = 60) -> str:
    """Collapse a message body to a single short line for the progress display."""
    flat = " ".join(str(text or "").split())
    return flat[: limit - 1] + "…" if len(flat) > limit else flat


def _rows(payload: dict) -> list[dict]:
    """+record-list returns a column matrix; zip it back into named rows keyed by record_id."""
    fields, data, ids = payload.get("fields") or [], payload.get("data") or [], payload.get("record_id_list") or []
    return [{"record_id": rid, **dict(zip(fields, row, strict=False))} for rid, row in zip(ids, data, strict=False)]


# `html.unescape` also decodes entities written without a semicolon, so a URL carrying
# `&timestamp=` or `&notify=` silently loses those characters to the `times` and `not`
# entities. Lark always emits the semicolon form, so only that is safe to decode.
RE_ENTITY = compile(r"&(#\d+|#[xX][0-9a-fA-F]+|\w+);")


def _clean(value):
    """Lark's search endpoints return HTML-entity-escaped text with <h> hit markers.
    Left as-is those become `&lt;b&gt;` on disk, which breaks plain-text grep."""
    if isinstance(value, str):
        return RE_ENTITY.sub(lambda m: unescape(m[0]), value.replace("<h>", "").replace("</h>", ""))
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


async def sync_minutes(store: Store, p: Progress, *, since: str = "2023-01-01"):
    """Minutes (妙记): metadata, AI summary, chapters, todos, and full transcript.

    `+search` caps at 50 results per query no matter the filters, so a single call over
    the whole history silently reports 50. Walking month by month lifts the ceiling.
    """
    cli.current_group.set("minutes")
    p.set("minutes", state="running")
    found: dict[str, dict] = {}
    month = datetime.fromisoformat(since).replace(tzinfo=UTC)
    now = datetime.now(UTC)
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
        month = nxt

    p.set("minutes", total=len(found))
    for token, item in found.items():
        store.write_yaml(f"minutes/{token}/meta.yaml", _clean(item))
        p.bump("minutes", last=_oneline(_clean(item.get("display_info"))))

    todo = [t for t in found if not store.exists(f"minutes/{t}/transcript.txt")]
    p.set("minutes", note=f"{len(todo)} transcripts to fetch")

    async def detail(token: str):
        Aborted.check()
        try:
            # +detail writes transcript.txt into <cwd>/minutes/<token>/ itself -- run it in the store so it lands in place
            data = await cli.run("minutes", "+detail", "--minute-tokens", token, "--transcript", "--summary", "--todo", "--chapter", "--keyword", cwd=str(store.root))
        except cli.LarkError:
            return
        for m in (data or {}).get("minutes") or []:
            arts = m.get("artifacts") or {}
            store.write_yaml(f"minutes/{token}/detail.yaml", _clean({k: v for k, v in m.items() if k != "artifacts"}))
            if chapters := arts.get("chapters"):
                store.write_yaml(f"minutes/{token}/chapters.yaml", _clean(chapters))
            if todos := arts.get("todos"):
                store.write_yaml(f"minutes/{token}/todos.yaml", _clean(todos))
            if summary := arts.get("summary"):
                store.write(f"minutes/{token}/summary.md", summary if isinstance(summary, str) else str(summary))

    await gather(*(detail(t) for t in todo))
    p.set("minutes", state="done")


async def sync_meetings(store: Store, p: Progress, *, since: str = "2023-01-01"):
    """VC meeting records; links each meeting to its minute_token and note_id.

    Like minutes, `+search` has a per-query ceiling (150 here), so walk month by month.
    """
    cli.current_group.set("meetings")
    p.set("meetings", state="running")
    ids: list[str] = []
    month = datetime.fromisoformat(since).replace(tzinfo=UTC)
    now = datetime.now(UTC)
    while month < now:
        nxt = (month + timedelta(days=32)).replace(day=1)
        try:
            async for item in cli.paginate("vc", "+search", "--start", month.strftime("%Y-%m-%d"), "--end", nxt.strftime("%Y-%m-%d"), key="items"):
                if mid := item.get("id"):
                    ids.append(mid)
                    store.write_yaml(f"meetings/{mid}/meta.yaml", _clean(item))
                    p.bump("meetings", last=_oneline(_clean(item.get("display_info"))))
        except cli.LarkError as e:
            if not e.is_pagination_exhausted:
                p.set("meetings", state="error", note=_reason(e))
                return
        month = nxt

    todo = [i for i in ids if not store.exists(f"meetings/{i}/detail.yaml")]
    for batch in [todo[i : i + 20] for i in range(0, len(todo), 20)]:
        Aborted.check()
        try:
            data = await cli.run("vc", "+detail", "--meeting-ids", ",".join(batch))
        except cli.LarkError:
            continue
        for m in (data or {}).get("meetings") or []:
            store.write_yaml(f"meetings/{m['meeting_id']}/detail.yaml", _clean(m))
    p.set("meetings", state="done")


async def sync_bases(store: Store, p: Progress):
    """Bitables reachable from drive search; one JSONL of records per table."""
    cli.current_group.set("bases")
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
    cli.current_group.set("wiki")
    p.set("wiki", state="running")
    try:
        spaces = await cli.run("wiki", "+space-list", "--page-all")
    except cli.LarkError as e:
        p.set("wiki", state="error", note=str(e.payload)[:60])
        return
    items = (spaces or {}).get("spaces") or []
    p.set("wiki", total=len(items))

    async def level(sid: str, parent: str | None) -> list[dict]:
        argv = ["wiki", "+node-list", "--space-id", sid, "--page-all", *(["--parent-node-token", parent] if parent else [])]
        try:
            return ((await cli.run(*argv)) or {}).get("nodes") or []
        except cli.LarkError:
            return []

    async def walk(sid: str) -> list[dict]:
        """Wiki nodes form a tree and `+node-list` returns one level at a time.

        Breadth-first with a bounded fan-out per round: recursing with `gather` queues the
        whole subtree at once, which floods the API into a rate limit even though the
        semaphore caps what runs concurrently.
        """
        found: list[dict] = []
        frontier = [None]
        while frontier:
            batch, frontier = frontier[:cli.CONCURRENCY], frontier[cli.CONCURRENCY:]
            for nodes in await gather(*(level(sid, parent) for parent in batch)):
                found += nodes
                frontier += [n["node_token"] for n in nodes if n.get("has_child")]
        return found

    async def one(space: dict):
        sid = space.get("space_id")
        store.write_yaml(f"wiki/{sid}/meta.yaml", space)
        store.write_yaml(f"wiki/{sid}/nodes.yaml", _clean(await walk(sid)))
        p.bump("wiki")

    await gather(*(one(s) for s in items))
    p.set("wiki", state="done")


ALL = ["messages", "chats", "docs", "minutes", "meetings", "bases", "wiki"]


async def sync_all(root: Path, p: Progress, only: list[str] | None = None):
    """Run every collection concurrently.

    Two real dependencies exist -- chats wants the chat ids messages discovered, and docs
    mines wiki's node list for tokens that search misses -- but neither justifies blocking
    the whole run: each dependent awaits just its producer, so the independent collections
    make progress from the first second instead of idling behind the message sweep.
    """
    store = Store(root)
    want = set(only or ALL)
    tasks = []

    messages = create_task(sync_messages(store, p)) if "messages" in want else None
    wiki = create_task(sync_wiki(store, p)) if "wiki" in want else None

    async def chats():
        # a rate-limited message sweep must not take the roster down with it
        found = (await messages) or set() if messages else set()
        await sync_chat_meta(store, p, found)

    async def docs():
        if wiki:
            await wiki
        await sync_docs(store, p)

    if "chats" in want:
        tasks.append(chats())
    if "docs" in want:
        tasks.append(docs())
    if "minutes" in want:
        tasks.append(sync_minutes(store, p))
    if "meetings" in want:
        tasks.append(sync_meetings(store, p))
    if "bases" in want:
        tasks.append(sync_bases(store, p))

    # producers are awaited by their dependents; await them here only if nobody else will
    if messages and "chats" not in want:
        tasks.append(messages)
    if wiki and "docs" not in want:
        tasks.append(wiki)

    await gather(*tasks)
    store.save_cursors()
    return store
