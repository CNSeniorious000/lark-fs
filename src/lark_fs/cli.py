"""The request layer: every call goes out over the Open API itself, in-process.

The argv shapes below are still `lark-cli`'s -- they are the seam `sync.py` speaks, and
keeping them is what makes this a transport swap rather than a rewrite. What changed is
where a request lands: no process is spawned for one. `lark-cli` remains the credential
holder (`auth login`, refresh), and where its store cannot be read -- a platform whose
keychain is not the local file one -- the old subprocess path answers instead.

A call is one aiohttp request under the same gates as before: the semaphore, the search
rate gate, backoff on 99991400, and the abort on 99991403. Nothing about the retry or
feed behaviour moved; only the trip to the wire is shorter."""

from asyncio import CancelledError, Lock, Semaphore, create_subprocess_exec, create_task, gather, get_running_loop, shield, sleep, subprocess
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Collection
from contextlib import nullcontext, suppress
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta, timezone
from html import unescape
from itertools import count, pairwise
from json import JSONDecoder, loads
from os import environ
from pathlib import Path
from re import compile
from shutil import which
from time import monotonic
from typing import Any

from aiohttp import ClientSession, ClientTimeout, TCPConnector

# defined in abort.py, never here: a reload rebinds this module's classes, and then no
# `except SyncAbortedError` matches an exception raised before it (see abort.py)
from .abort import Aborted  # excluded from reload management, so importing it here is safe


def message_views(*args, **kwargs):
    """The message view port, reached through a wrapper rather than a module-level import.

    This file is re-executed on every reload, and a `from .messages import ...` at the top
    level would read that managed module through the reloader's reactive proxy mid-reload --
    a reactive read where none is expected, which corrupts its computation stack (the hmr
    entry test is what catches it). Imports of reloadable siblings belong inside functions.
    """
    from .messages import message_views as views

    return views(*args, **kwargs)


# The search endpoints rate-limit hard (that is what forced this down to 3), but the
# per-chat listing does not: 40 chats at 8-way concurrency sustained 88 msg/s with zero
# 429s, against 10 msg/s before. Raise this only with a measurement, never a guess.
CONCURRENCY = 8


class RateGate:
    """Admit at most `limit` requests per rolling `seconds`, whoever is asking.

    The document search is limited to 100 a minute (its own docs say so), and that is a
    budget, not a width: fanning windows out over the shared semaphore blew through it at
    width 3 and lost 2840 of 4762 hits to 99991400, while one sequential walk at ~1.15/s
    never touched it. A gate spends the same budget with as many requests in flight as
    the semaphore allows, so the walk runs at the endpoint's ceiling instead of under it.
    """

    def __init__(self, limit: int, seconds: float):
        self.limit, self.seconds = limit, seconds
        self.stamps: deque[float] = deque()
        self.lock = Lock()  # one waiter at a time, so a freed slot admits one request and not the whole queue

    async def __aenter__(self):
        async with self.lock:
            while True:
                now = monotonic()
                while self.stamps and now - self.stamps[0] >= self.seconds:
                    self.stamps.popleft()
                if len(self.stamps) < self.limit:
                    self.stamps.append(now)
                    return
                await sleep(self.stamps[0] + self.seconds - now)

    async def __aexit__(self, *_):
        pass


_sem: Semaphore
search_gate: RateGate


def reset_loop_state():
    """Rebuild the primitives that belong to one event loop.

    asyncio binds a Semaphore or Lock to the loop that first *contends* on it, and hmr.py
    runs every cycle in its own `asyncio.run`. A reload only re-executes the files that
    changed, so editing any other module leaves these two pinned to the loop that just
    died and the next sweep fails with "bound to a different event loop". Constructing
    them here at cycle boundaries leaves in-flight requests on the same semaphore and
    gate until they finish. The new gate uses the current methods but retains timestamps:
    a code reload must not grant a fresh minute of API budget.
    """
    global _sem, search_gate
    _sem = Semaphore(CONCURRENCY)
    old_gate = globals().get("search_gate")
    search_gate = RateGate(90, 60.0)  # 90 of the documented 100 a minute, so the retry that follows a refusal has room to land
    if old_gate is not None:
        search_gate.stamps = old_gate.stamps


# Re-executing this module must not hand out fresh slots while old requests still hold
# theirs. Only first import initializes here; hmr.py resets after the old cycle has ended.
# The second branch is the other ordering: a reload's re-exec lands *after* that reset
# already ran, rebinding these classes under primitives the old implementation built --
# so adopt them again, but only with every slot free, which is the same invariant.
# with every slot free -- which is the same invariant -- an implementation the reload
# rebound also adopts the primitives once more.
_state = globals()
if "_sem" not in _state or (type(_state.get("search_gate")) is not RateGate and _state["_sem"]._value == CONCURRENCY):  # noqa: SLF001 -- the slot count is the invariant
    reset_loop_state()

FEED_LIMIT = 40  # per-group history depth; the TUI decides how much of it to show


class Activity:
    """A rolling log of requests, grouped by the collection that issued them.

    Within a group, finished entries drift up and out while running ones stay pinned at
    the bottom, so a row never jumps position the moment it completes.
    """

    def __init__(self):
        self.done: dict[str, deque[tuple[str, str, str]]] = defaultdict(lambda: deque(maxlen=FEED_LIMIT))
        self.running: dict[int, tuple[str, str, str]] = {}

    def start(self, rid: int, group: str, domain: str, subject: str):
        self.running[rid] = (group, domain, subject)

    def finish(self, rid: int, state: str, result: str = ""):
        """`result` describes what came back -- a title, a name, a count -- which is far
        more useful than the request parameters once a call has completed."""
        if entry := self.running.pop(rid, None):
            group, domain, subject = entry
            # an opaque token adds nothing once the reply names what it held, but a time
            # window is that request's identity -- drop the former, keep the latter
            verb, _, arg = subject.partition(" ")
            keep = arg if RE_DATE.fullmatch(arg) else ""
            head = f"{verb} {keep}".strip()
            self.done[group].append((domain, f"{head} {result}" if result else subject, state))

    def rows(self, group: str, limit: int) -> list[tuple[str, str, str]]:
        live = [(d, s, "running") for g, d, s in self.running.values() if g == group]
        return (list(self.done.get(group, ())) + live)[-limit:]

    def busy(self, group: str) -> int:
        return sum(1 for g, _, _ in self.running.values() if g == group)


activity = Activity()
feed_enabled = False  # set by the TUI; the plain renderer never reads the feed
current_group: ContextVar[str] = ContextVar("current_group", default="")  # which collection issued this request
_next_id = count()


TITLE_KEYS = ("name", "title", "topic", "display_info", "chat_name", "title_highlighted")
LIST_KEYS = ("chats", "results", "messages", "nodes", "tables", "spaces", "users", "records", "minutes", "meetings", "items")


def _summarise(payload) -> str:
    """Describe what a response actually contained, for the activity feed.

    A request's parameters are opaque ids; the reply carries names and counts, which is
    what makes a line worth reading. Single results show their title, lists show a count
    plus the first title so consecutive pages stay distinguishable.
    """
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return ""
    for key in LIST_KEYS:
        if isinstance(items := data.get(key), list):
            if not items:
                return "nothing"  # an empty window is a result too; saying so beats echoing the query
            head = _title(items[0])
            label = "" if key == "items" else f" {key}"
            return f"{len(items)}{label}" + (f" · {head}" if head else "")
    return _title(data)


def oneline(text, limit: int = 60) -> str:
    """Collapse to a single line and bound its length, for progress display."""
    flat = " ".join(str(text or "").split())
    return flat[: limit - 1] + "…" if len(flat) > limit else flat


def summarise_title(text) -> str:
    """One-line display form of a title: markup stripped, keyword blurb dropped."""
    return oneline(RE_MARKUP.sub("", unescape_entities(str(text or ""))).split("关键词")[0], 48)


def _title(item) -> str:
    if not isinstance(item, dict):
        return ""
    for key in TITLE_KEYS:
        if isinstance(v := item.get(key), str) and v.strip():
            return summarise_title(v)
    if isinstance(nested := item.get("result_meta"), dict):
        return _title(nested)  # drive search nests the fields one level down
    return ""


RE_ENTITY = compile(r"&(#\d+|#[xX][0-9a-fA-F]+|\w+);")


def unescape_entities(text: str) -> str:
    """Decode only the semicolon form. `html.unescape` also decodes entities written
    without one, so a URL carrying `&timestamp=` or `&notify=` silently loses characters.
    """
    return RE_ENTITY.sub(lambda m: unescape(m[0]), text)


# Every tenant has its own domain and nothing in the API reports it -- `auth status` gives
# an app id and an open id, not a host. `sync` fills this in from a URL Lark has already
# handed us; until then, set it here or in LARK_FS_TENANT (e.g. https://acme.feishu.cn).
TENANT = environ.get("LARK_FS_TENANT", "")


def link_for(token: str) -> str:
    """Best-effort Feishu URL for an opaque token, so a feed line can be clicked.

    Prefixes are the only signal available -- the CLI takes bare tokens and never tells us
    what kind of object they name -- but they are stable and unambiguous in practice.
    Everything but applink needs the tenant's own domain; without it those stay plain text.
    """
    if token.startswith("oc_"):
        return f"https://applink.feishu.cn/client/chat/open?openChatId={token}"
    if token.startswith("ou_"):
        return f"https://applink.feishu.cn/client/contact/open?openId={token}"
    if not TENANT:
        return ""
    if token.startswith("obc"):
        return f"{TENANT}/minutes/{token}"
    if token.startswith(("tbl", "bas")):
        return f"{TENANT}/base/{token}"
    if token.isdigit():
        return f"{TENANT}/wiki/settings/{token}" if len(token) > 15 else ""
    return f"{TENANT}/wiki/{token}"


def _label(argv: tuple[str, ...]) -> tuple[str, str]:
    """Split a command into (domain, subject) so the feed can align them in columns.

    `("minutes", "+detail", "--minute-tokens", "obc...")` -> `("minutes", "+detail obc...")`.
    The subject takes an id-looking flag value -- an opaque token, not an enum-ish option
    like `markdown` or `p2p,group` -- preferring the last, since a child node token
    identifies the work better than the parent space it lives under.
    """
    domain, *rest = argv
    verb = rest[0].lstrip("+") if rest else ""
    # a time-windowed sweep is better identified by its window than by any token in it
    if start := (flags := dict(pairwise(argv))).get("--start") or flags.get("--created-since"):
        return domain, f"{verb} {start[:10]}"
    ids = [v for f, v in pairwise(argv) if f.startswith("--") and RE_ID.fullmatch(v)]
    return domain, f"{verb} {ids[-1]}".strip() if ids else verb


RE_ID = compile(r"(?=[A-Za-z0-9_-]*\d)[A-Za-z0-9_-]{8,}")
RE_MARKUP = compile(r"</?[a-zA-Z][^>]*>")
RE_PAGE_CAP = compile(r"--page-size \d+: must be between 1 and (\d+)")
RE_DATE = compile(r"\d{4}-\d{2}-\d{2}")
RE_CODE = compile(r'"code":\s*(\d+)')


class LarkError(Exception):
    def __init__(self, argv: list[str], payload: Any):
        self.argv, self.payload = argv, payload
        super().__init__(f"{' '.join(argv)} -> {payload}")

    @property
    def missing_scopes(self) -> list[str]:
        return (self.payload or {}).get("error", {}).get("missing_scopes", []) if isinstance(self.payload, dict) else []

    @property
    def _code(self) -> int | None:
        """lark-cli sometimes truncates its error JSON mid-object, so the structured code
        is unavailable and the raw text is all we have. Read it from whichever survives."""
        err = self.payload.get("error", {}) if isinstance(self.payload, dict) else {}
        if isinstance(err.get("code"), int):
            return err["code"]
        found = RE_CODE.search(str(err.get("message", "")))
        return int(found[1]) if found else None

    @property
    def page_size_cap(self) -> int | None:
        """The endpoint's own limit, when it rejected our page size for being too large."""
        found = RE_PAGE_CAP.search(str(self.payload))
        return int(found[1]) if found else None

    @property
    def is_pagination_exhausted(self):
        """121022: the endpoint refuses to page further. Expected on wide windows --
        it means "that's all you get here", not that the collection failed."""
        return self._code == 121022

    @property
    def is_rate_limited(self):
        # 99991400 is Lark's "request trigger frequency limit"; the subtype spelling varies by endpoint
        return self._code == 99991400 or "rate_limit" in str(self.payload)

    @property
    def is_missing(self):
        """This endpoint will never have anything for that token.

        Permanent, unlike a rate limit -- worth remembering, so the same request is not
        reissued on every run. Drive says 1069307 for a token it does not have, and 1069304
        (comments) or 3380003 (body) for a document that has been deleted; Base says 91402,
        and 800004006 for a token that was never a base to begin with, which the document
        corpus hands it a couple of.
        """
        return self._code in (1069307, 1069304, 3380003, 91402, 800004006)

    @property
    def is_unsupported_type(self):
        """3380002: this token names a sheet or a bitable, and only docx exports markdown.

        Permanent, like `is_missing`. Measured before this was told apart from a rate
        limit: 146 sheets and bitables, re-fetched on every single sync and failing
        identically every time -- 31% of what a sync cost once the discovery passes were
        put on a schedule.
        """
        return self._code == 3380002

    @property
    def is_unlistable_type(self):
        """lark-cli refused to send it: the token resolves to a kind the comments endpoint
        does not name. There is no API code because there was no API call.

        Permanent, like `is_missing`, and the mirror holds one -- a mindnote, which no
        `--type` reaches. Left unrecognised it is a request every run, forever.
        """
        return "comments list only supports" in str(self.payload)

    @property
    def is_undeliverable(self):
        """lark-cli could not bring the bytes across, and will not next time either.

        Its ranged download mis-frames the response for some attachments -- "range response
        delivered more than the N bytes its framing declared", reproducible on the same file
        twice -- and a handful answer HTTP 400 outright. 15 of the 15587 attachments on this
        store are in that state, and they were 15 downloads on every single sync.

        Not the tenant's answer but the client's, so a `lark-cli update` may well fix it:
        remembered as a clock rather than a verdict.
        """
        return "its framing declared" in str(self.payload) or "HTTP 400" in str(self.payload)

    @property
    def is_wrong_kind(self):
        """131005: the token is real and is not the kind it was asked for.

        A `/wiki/` URL carries a node token for some documents and the document's own for
        others, and nothing in the link says which. Unlike 1069307 -- which means the same
        thing *and* "no such token" -- this one is safe to retry with another type.
        """
        return self._code == 131005

    @property
    def is_forbidden(self):
        """The account can see that this exists and cannot read it.

        One thing, a different shape per endpoint: `docs +fetch` answers 3380004, `drive
        +list-comments` 131006 or 1069303, `note +detail` 121005, and `minutes +detail` says
        so in the body per token rather than as a code, offering to ask the owner -- which
        this will not do on anyone's behalf. 189 of the 710 minutes the meetings name are in
        this state, and they were 189 requests every sync.

        Unlike `is_missing` this can stop being true, so it is remembered as a clock.
        """
        return self._code in (3380004, 131006, 121005, 1069303) or "No read permission" in str(self.payload)

    @property
    def is_quota_exhausted(self):
        """99991403: the tenant's *monthly* API allowance is gone.

        A different ceiling from 99991400 and a far worse one -- it counts every custom
        app in the tenant together, resets on the 1st, and no amount of backoff clears it.
        Retrying is not just useless, it burns the next month's budget once it rolls over.
        """
        return self._code == 99991403


def _binary() -> str:
    """What to exec when the fallback path runs, skipping the npm shim when PATH serves one.

    `lark-cli` on PATH is usually `scripts/run.js` -- a node script whose only real job is
    `execFileSync("../bin/lark-cli", args)`, a V8 cold start per request. The binary always
    sits beside the shim, so follow the symlink and take it.
    """
    found = which("lark-cli") or "lark-cli"
    real = Path(found).resolve()
    if real.suffix == ".js":
        native = real.parent.parent / "bin" / "lark-cli"
        if native.is_file():
            return str(native)
    return found


BIN = _binary()


def _parse(text: str) -> Any | None:
    """Extract lark-cli's JSON body, for the fallback path.

    Some shortcuts print progress lines before it and trailing output after it, so decode
    the first complete object rather than requiring the whole tail to be valid JSON.
    """
    for i, ch in enumerate(text):
        if ch == "{":
            try:
                return JSONDecoder().raw_decode(text, i)[0]
            except ValueError:
                continue
    return None


# --- the wire: one in-process request per call -----------------------------------------------

_UNSET = object()
creds: Any = _UNSET  # `_credentials()` fills it; None means "no readable store here", and the fallback covers that
_session: ClientSession | None = None
_session_loop = 0
_tenant: tuple[str, float] = ("", 0.0)


def _applink() -> str:
    brand = getattr(_credentials(), "brand", "") or "feishu"
    return "https://applink.larksuite.com" if brand.startswith("lark") else "https://applink.feishu.cn"


def _tokens():
    from . import tokens  # same reload note as `message_views`

    return tokens


def _credentials():
    """lark-cli's store, read on first use (importing `tokens` at module exec: see `message_views`)."""
    global creds
    if creds is _UNSET:
        creds = _tokens().load()
    return creds


def _host() -> str:
    """Every tenant has its own domain; `brand` is the only thing that names it."""
    creds = _credentials()
    return "https://open.larksuite.com" if (creds.brand if creds else "feishu").startswith("lark") else "https://open.feishu.cn"


async def _client():
    """One aiohttp session per event loop -- connections are the whole point -- closed when
    a reload brings a new loop, since a session cannot outlive the loop it opened on."""
    global _session, _session_loop
    loop = get_running_loop()
    if _session is None or _session_loop != id(loop):
        if _session is not None:
            with suppress(Exception):
                await _session.close()
        _session = ClientSession(connector=TCPConnector(limit=CONCURRENCY))
        _session_loop = id(loop)
    return _session


async def _user_token() -> str:
    """The user access token, refreshed in place when it is within minutes of expiring.

    The rotated refresh token is written back to lark-cli's own record (under its lock),
    or the next refresh on either side fails against a token the server has retired.
    """
    global creds
    creds = _credentials()
    if creds is None:
        raise LarkError([], {"error": {"message": "no lark-cli credentials to call with"}})
    if creds.user_token_usable:
        return creds.access_token
    session = await _client()
    async with session.post(_host() + "/open-apis/authen/v2/oauth/token", data=_tokens().refresh_body(creds), headers={"Content-Type": "application/json"}) as resp:
        payload = await _payload(resp)
    if not isinstance(payload, dict) or not payload.get("access_token"):
        raise LarkError([], {"error": {"message": f"token refresh failed: {str(payload)[:200]}"}})
    creds = _tokens().save(creds, _tokens().parse_token_reply(payload))
    return creds.access_token


async def _bot_token() -> str:
    """A tenant token, cached for its lifetime. Rare: only `--as bot` calls ask for one."""
    global _tenant
    creds = _credentials()
    if creds is None:
        raise LarkError([], {"error": {"message": "no lark-cli credentials to call with"}})
    if _tenant[0] and _tenant[1] > monotonic():
        return _tenant[0]
    session = await _client()
    async with session.post(_host() + "/open-apis/auth/v3/tenant_access_token/internal", json={"app_id": creds.app_id, "app_secret": creds.app_secret}) as resp:
        payload = await _payload(resp)
    token = (payload or {}).get("tenant_access_token")
    if not token:
        raise LarkError([], {"error": {"message": f"tenant token failed: {str(payload)[:200]}"}})
    _tenant = (token, monotonic() + max(60, float((payload or {}).get("expire") or 600) - 120))
    return token


async def _payload(resp) -> Any:
    try:
        return await resp.json(content_type=None)
    except ValueError, UnicodeDecodeError:
        return {"error": {"message": f"HTTP {resp.status}: non-JSON response"}}


async def session_scope(coro):
    """Run the app's work, then release the HTTP session with it -- a session that
    outlives its loop is an "Unclosed client session" in the log at exit."""
    try:
        return await coro
    finally:
        global _session
        if _session is not None:
            with suppress(Exception):
                await _session.close()
            _session = None


def _writer(out: Path):
    """Opened outside the async body on purpose: the sink for a streamed download."""
    out.parent.mkdir(parents=True, exist_ok=True)
    return out.open("wb")


async def _save(out: Path, content) -> dict:
    """Stream a download to disk -- Lark never reports a size in advance, so the bytes
    are counted on the way through and the cap is enforced after the fact."""
    size = 0
    with _writer(out) as f:
        async for chunk in content.iter_chunked(1 << 16):
            f.write(chunk)
            size += len(chunk)
    return {"saved_path": str(out), "size_bytes": size}


async def request(method: str, path: str, params: dict | None = None, body: Any = None, *, as_: str = "user", out: Path | None = None) -> Any:
    """One Open API call. Returns the response's `data`; raises LarkError shaped like
    lark-cli's own `{"error": ...}`, so every classifier on it keeps meaning the same.

    `out` streams the response body to that file instead -- attachments have no JSON.
    """
    Aborted.check()
    session = await _client()
    token = await _user_token() if as_ == "user" else await _bot_token()
    url = _host() + path if path.startswith("/open-apis") else path
    async with session.request(method, url, params=params, json=body, headers={"Authorization": f"Bearer {token}"}, timeout=ClientTimeout(total=300)) as resp:
        if out is not None:
            if resp.status >= 400:
                raise LarkError([method, path], {"error": {"message": f"HTTP {resp.status} downloading {path}"}})
            return await _save(out, resp.content)
        payload = await _payload(resp)
    if resp.status >= 400 and not isinstance(payload, dict):
        raise LarkError([method, path], {"error": {"message": f"HTTP {resp.status}: {str(payload)[:200]}"}})
    if isinstance(payload, dict) and payload.get("code"):
        error = {"code": payload["code"], "message": payload.get("msg") or str(payload)[:200]}
        if missing := payload.get("missing_scopes") or (payload.get("error") or {}).get("missing_scopes"):
            error["missing_scopes"] = missing
        raise LarkError([method, path], {"error": error})
    return payload.get("data") if isinstance(payload, dict) else payload


# --- argv to request: the lark-cli command shapes `sync.py` speaks ----------------------------

MESSAGE_PARAMS = {"with_sender_name": "true", "card_msg_content_type": "raw_card_content"}
TENANT_TZ = timezone(timedelta(hours=8))


def _flags(argv: tuple[str, ...]) -> tuple[list[str], dict[str, Any]]:
    """Positionals first, then `--flag value` pairs; a flag with no value reads as True."""
    pos: list[str] = []
    flags: dict[str, Any] = {}
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok.startswith("--"):
            if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                flags[tok] = argv[i + 1]
                i += 2
            else:
                flags[tok] = True
                i += 1
        else:
            pos.append(tok)
            i += 1
    return pos, flags


def _page(flags: dict[str, Any]) -> dict[str, Any]:
    return {k: flags[f"--{k}"] for k in ("page_size", "page_token") if f"--{k}" in flags}


def _stamp(value: str) -> str:
    """A `%Y-%m-%d %H:%M` stamp on the tenant's wall clock, as the epoch seconds the API wants."""
    return str(int(datetime.strptime(value[:16], "%Y-%m-%d %H:%M").replace(tzinfo=TENANT_TZ).timestamp())) if value else ""


async def _dispatch(argv: tuple[str, ...], cwd: str | None) -> Any:
    pos, flags = _flags(argv)
    head = tuple(pos[:3]) if len(pos) >= 3 else tuple(pos)

    if head[:1] == ("api",):
        method, path = pos[1], pos[2]
        params = loads(flags["--params"]) if "--params" in flags else {}
        params |= _page(flags)
        return await request(method, path, params=params or None, body=loads(flags["--data"]) if "--data" in flags else None, as_=flags.get("--as", "user"))

    if head[:2] == ("im", "+chat-list"):
        data = await request("GET", "/open-apis/im/v1/chats", params={"types": flags.get("--types", ""), "sort_type": "ByCreateTimeAsc", "user_id_type": "open_id", **_page(flags)}) or {}
        chats = [{**c, "chat_app_link": f"{_applink()}/client/chat/open?openChatId={c['chat_id']}"} for c in data.pop("items", []) if c.get("chat_id")]
        return {**data, "chats": chats}

    if head[:2] == ("im", "+chat-messages-list"):
        params = {
            "container_id_type": "chat",
            "container_id": flags["--chat-id"],
            "sort_type": "ByCreateTimeDesc" if flags.get("--order") == "desc" else "ByCreateTimeAsc",
            "only_thread_root_messages": "true",
            **MESSAGE_PARAMS,
            **_page(flags),
        }
        for name, key in (("--start", "start_time"), ("--end", "end_time")):
            if flags.get(name):
                params[key] = _stamp(flags[name])
        data = await request("GET", "/open-apis/im/v1/messages", params=params) or {}
        views = await message_views(data.get("items") or [], get=request, reactions="--no-reactions" not in flags)
        return {**{k: v for k, v in data.items() if k != "items"}, "messages": views}

    if head[:2] == ("im", "+threads-messages-list"):
        thread = flags["--thread"]
        if not thread.startswith("omt_"):  # a message id: the thread it belongs to is what pages
            row = ((await request("GET", "/open-apis/im/v1/messages/mget", params={"message_ids": [thread], **MESSAGE_PARAMS})) or {}).get("items") or []
            thread = (row[0].get("thread_id") if row else "") or thread
        params = {"container_id_type": "thread", "container_id": thread, "sort_type": "ByCreateTimeDesc" if flags.get("--order") == "desc" else "ByCreateTimeAsc", **MESSAGE_PARAMS, **_page(flags)}
        data = await request("GET", "/open-apis/im/v1/messages", params=params) or {}
        views = await message_views(data.get("items") or [], get=request, reactions="--no-reactions" not in flags)
        return {**{k: v for k, v in data.items() if k != "items"}, "messages": views}

    if head[:2] == ("im", "+messages-mget"):
        ids = [m for m in str(flags["--message-ids"]).split(",") if m]
        data = await request("GET", "/open-apis/im/v1/messages/mget", params={"message_ids": ids, **MESSAGE_PARAMS}) or {}
        views = await message_views(data.get("items") or [], get=request, reactions="--no-reactions" not in flags)
        return {"messages": views}

    if head[:2] == ("im", "+chat-members-list"):
        # `--page-all` is not all of them: lark-cli stopped after ten pages unless told
        # otherwise, so the walk is per page and the answer is one merged roster
        roster: dict[str, list] = {"users": [], "bots": []}
        last: dict = {}

        async def page(token: str) -> dict:
            return (
                await request("GET", f"/open-apis/im/v1/chats/{flags['--chat-id']}/members/list", params={"member_id_type": "open_id", "page_size": 100, **({"page_token": token} if token else {})})
                or {}
            )

        token = ""
        while True:
            last = await page(token)
            for key in roster:
                roster[key] += last.get(key) or []
            if not (last.get("has_more") and (token := last.get("page_token") or "")):
                break
        return {**last, **roster, "chat_id": flags["--chat-id"], "has_more": False, "page_token": ""}

    if head[:2] == ("im", "+messages-resources-download"):
        dest = Path(cwd or ".") / flags["--output"]
        return await request("GET", f"/open-apis/im/v1/messages/{flags['--message-id']}/resources/{flags['--file-key']}", params={"type": flags.get("--type", "file")}, out=dest)

    if head[:2] == ("contact", "+get-user"):
        return {"user": await request("GET", "/open-apis/authen/v1/user_info")}

    if head[:3] == ("drive", "metas", "batch_query"):
        return await request("POST", "/open-apis/drive/v1/metas/batch_query", body=loads(flags["--data"]))

    if head[:2] == ("drive", "+search"):
        window = {}
        if flags.get("--created-since"):
            window = {"create_time": {"start": int(flags["--created-since"]), "end": int(flags["--created-until"])}}
        body = {"query": flags.get("--query", ""), "page_size": int(flags.get("--page-size") or 15), "doc_filter": window, "wiki_filter": window}
        if flags.get("--page-token"):
            body["page_token"] = flags["--page-token"]
        data = await request("POST", "/open-apis/search/v2/doc_wiki/search", body=body) or {}
        # the recall calls its hits `res_units`, and each carries the three epoch times
        # twice: raw, and re-read as an ISO stamp for whoever displays them
        results = [{**unit, "result_meta": _iso_times(unit.get("result_meta") or {})} for unit in data.pop("res_units", [])]
        return {"results": results, "total": data.get("total"), "has_more": data.get("has_more"), "page_token": data.get("page_token"), **({"notice": data["notice"]} if data.get("notice") else {})}

    if head[:2] == ("drive", "+list-comments"):
        params = {"file_type": flags["--type"], "page_size": int(flags.get("--page-size") or 50), **({"page_token": flags["--page-token"]} if flags.get("--page-token") else {})}
        if flags.get("--solved-status") and flags["--solved-status"] != "all":
            params["solved_status"] = flags["--solved-status"]
        data = await request("GET", f"/open-apis/drive/v1/files/{flags['--token']}/comments", params=params) or {}
        return {**data, "count": len(data.get("items") or []), "file_token": flags["--token"], "file_type": flags["--type"]}

    if head[:2] == ("docs", "+fetch"):
        body = {
            "format": flags.get("--doc-format", "markdown"),
            "lang": "zh_cn",
            "extra_param": '{"enable_user_cite_reference_map":true,"include_comments":true,"return_html5_block_data":true}',
            "export_option": {"export_block_id": False, "export_cite_extra_data": False, "export_style_attrs": False},
        }
        return await request("POST", f"/open-apis/docs_ai/v1/documents/{flags['--doc']}/fetch", body=body)

    if head[:2] == ("minutes", "+search"):
        body = {"filter": {"create_time": {"start_time": f"{flags['--start']}T00:00:00Z", "end_time": f"{flags['--end']}T23:59:59Z"}}, "sorter": "create_time_desc"}
        return await request(
            "POST",
            "/open-apis/minutes/v1/minutes/search",
            params={"page_size": int(flags.get("--page-size") or 15), **({"page_token": flags["--page-token"]} if flags.get("--page-token") else {})},
            body=body,
        )

    if head[:3] == ("minutes", "minutes", "get"):
        return await request("GET", f"/open-apis/minutes/v1/minutes/{flags['--minute-token']}")

    if head[:2] == ("vc", "+search"):
        body = {"meeting_filter": {"start_time": {"start_time": f"{flags['--start']}T00:00:00Z", "end_time": f"{flags['--end']}T23:59:59Z"}}, "sorter": "start_time_desc"}
        return await request(
            "POST",
            "/open-apis/vc/v1/meetings/search",
            params={"page_size": int(flags.get("--page-size") or 15), **({"page_token": flags["--page-token"]} if flags.get("--page-token") else {})},
            body=body,
        )

    if head[:2] == ("vc", "+recording"):
        rows: list[dict] = []
        ids = [m for m in str(flags["--meeting-ids"]).split(",") if m]

        async def one(mid: str):
            try:
                rec = (await request("GET", f"/open-apis/vc/v1/meetings/{mid}/recording") or {}).get("recording") or {}
                row = {"meeting_id": mid}
                if url := rec.get("url"):
                    row["recording_url"] = url
                    # the minute a recording belongs to lives in its URL, and only there
                    if found := RE_MINUTE_URL.search(url):
                        row["minute_token"] = found[1]
                if rec.get("duration"):
                    row["duration"] = rec["duration"]
                rows.append(row)
            except LarkError as e:
                # one meeting without a recording says nothing about the others in the batch
                rows.append({"meeting_id": mid, "error": _reason(e)})

        await spread(one, ids)
        if rows and all("error" in r for r in rows):
            raise LarkError(list(argv), {"error": {"message": rows[0]["error"]}})
        return {"recordings": rows}

    if head[:3] == ("vc", "meeting", "get"):
        params = {"with_participants": "true"} if "--with-participants" in flags else {}
        return await request("GET", f"/open-apis/vc/v1/meetings/{flags['--meeting-id']}", params=params or None)

    if head[:2] == ("base", "+table-list"):
        return await request("GET", f"/open-apis/base/v3/bases/{flags['--base-token']}/tables", params={"limit": int(flags.get("--limit") or 100), "offset": 0})

    if head[:2] == ("base", "+record-list"):
        return await request(
            "GET",
            f"/open-apis/base/v3/bases/{flags['--base-token']}/tables/{flags['--table-id']}/records",
            params={"limit": int(flags.get("--limit") or 200), "offset": int(flags.get("--offset") or 0)},
        )

    if head[:2] == ("wiki", "+space-list"):
        collected: list[dict] = []

        async def page(token: str) -> dict:
            return await request("GET", "/open-apis/wiki/v2/spaces", params={"page_size": 50, **({"page_token": token} if token else {})}) or {}

        async def each(token: str) -> dict:
            data = await page(token)
            collected.extend(data.get("items") or [])
            return data

        token = ""
        while True:
            data = await each(token)
            if not (data.get("has_more") and (token := data.get("page_token") or "")):
                break
        data.pop("items", None)
        return {**data, "spaces": collected}

    return await _dispatch_subprocess(argv, cwd)  # a shape this table does not know: lark-cli still does


RE_MINUTE_URL = compile(r"/minutes/([A-Za-z0-9]+)")


def _iso_times(meta: dict) -> dict:
    """The epoch fields re-read as `2026-10-07T16:41:49Z`, beside the raw seconds."""
    out = dict(meta)
    for key in ("create_time", "update_time", "last_open_time"):
        if value := meta.get(key):
            out[f"{key}_iso"] = datetime.fromtimestamp(int(value), UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return out


def _reason(e: LarkError) -> str:
    payload = e.payload if isinstance(e.payload, dict) else {}
    return str((payload.get("error") or {}).get("message") or "failed")


async def _exchange(argv: tuple[str, ...], cwd: str | None) -> dict:
    """The seam `run` talks to: one call, as the `{"ok", "data"}` envelope lark-cli printed."""
    call = _dispatch if _credentials() is not None else _dispatch_subprocess  # without a readable store the subprocess is still the client
    try:
        return {"ok": True, "data": await call(argv, cwd)}
    except LarkError as e:
        return {"ok": False, "error": (e.payload or {}).get("error") or {"message": str(e)}}


async def _dispatch_subprocess(argv: tuple[str, ...], cwd: str | None) -> Any:
    args = [BIN, *argv, "--format", "json"]
    proc = await create_subprocess_exec(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    try:
        out, stderr = await proc.communicate()
    except CancelledError:
        # ctrl-c: kill the child and *wait for it*, or asyncio reaps the transport after
        # the loop has closed and complains "Loop ... is closed".
        with suppress(ProcessLookupError):
            proc.kill()
            with suppress(CancelledError):
                await shield(proc.wait())
        raise
    payload = _parse(out.decode(errors="replace")) or _parse(stderr.decode(errors="replace"))
    if payload is None:
        raise LarkError(list(argv), {"error": {"message": (out.decode(errors="replace") or stderr.decode(errors="replace") or f"exit {proc.returncode}")[:400]}})
    if payload.get("ok"):
        return payload.get("data")
    raise LarkError(list(argv), payload)


async def run(*argv: str, retries: int = 5, cwd: str | None = None, subject: str = "") -> Any:
    """One lark-cli-shaped call, over the wire in-process. Returns the `data` field."""
    Aborted.check()
    delay = 2.0
    for attempt in range(retries):
        # the budget is waited out before a slot is taken, so a search that is only waiting
        # for its minute does not hold one of the eight slots every other collection needs
        async with search_gate if argv[:2] == ("drive", "+search") else nullcontext(), _sem:
            rid = next(_next_id)
            if feed_enabled:
                domain, guessed = _label(argv)
                activity.start(rid, current_group.get(), domain, subject or guessed)
            failed = True
            outcome = ""
            payload = None
            try:
                payload = await _exchange(argv, cwd)
                failed = False
                outcome = "" if subject else (_summarise(payload) if feed_enabled else "")
            finally:
                if feed_enabled:
                    activity.finish(rid, "error" if failed else "done", outcome)
        if payload is None:
            raise LarkError(list(argv), {"error": {"message": "no answer"}})
        if payload.get("ok"):
            return payload.get("data")
        exc = LarkError(list(argv), payload)
        if exc.is_quota_exhausted:
            Aborted.flag = True  # nothing will succeed until the month rolls over
            # every collection catches LarkError, so the one that hit this swallows it and
            # the run ends at the next checkpoint as a plain abort -- which reads as "rerun
            # to resume", the one thing that cannot work until the 1st
            Aborted.reason = "the tenant's monthly API quota is spent; it resets on the 1st"
            raise exc
        if not exc.is_rate_limited or attempt == retries - 1:
            raise exc
        await sleep(delay)
        delay *= 2


async def paginate(*argv: str, key: str, page_size: int = 50, prefetch: bool = False, notices: list[str] | None = None):
    """Yield items across pages for shortcuts exposing --page-token/--page-size.

    With `prefetch`, the next page is requested while the current one is still being
    consumed. A page arrives in milliseconds but the next request takes a second or
    two, so without this the caller does all its work in one burst and then idles --
    which reads as a frozen counter rather than steady progress.

    A caller that stops early -- `sync_messages` does, once a chat passes its slice
    threshold -- leaves a prefetch in flight. The `finally` cancels it, so the answer is
    not paid for and thrown away, and the task does not outlive the generator that owns
    it. Reaching that `finally` at a useful moment is the caller's half of the deal:
    abandoning an async generator defers its cleanup to garbage collection, so consumers
    that may break out early wrap it in `contextlib.aclosing`.

    `notices` collects what the search endpoints say about their own answer. The response
    carries a `notice` string the human-readable docs never mention -- the generated SDKs
    do, with "搜索结果不全" as its example -- and it is the only in-band signal that a page
    is a partial one. `has_more: false` says the paging ended, not that the answer was whole.
    """

    size = page_size

    async def fetch(token: str | None):
        nonlocal size
        for _ in range(2):
            try:
                return await run(*argv, "--page-size", str(size), *(["--page-token", token] if token else []))
            except LarkError as e:
                if e.is_pagination_exhausted:
                    return None
                if (cap := e.page_size_cap) and cap < size:
                    size = cap
                    continue
                raise
        return None

    upcoming = None
    try:
        data = await fetch(None)
        while data:
            items = data.get(key) or []
            if notices is not None and isinstance(notice := data.get("notice"), str) and notice:
                notices.append(notice)
            token = data.get("page_token")
            upcoming = None
            if token and data.get("has_more"):
                # start the next request before handing out this page, so the caller's work
                # overlaps the wait instead of finishing in a burst and then idling
                upcoming = create_task(fetch(token)) if prefetch else None
            else:
                token = None

            pace = 1.0 / (len(items) or 1) if upcoming else 0
            for item in items:
                yield item
                if pace:
                    await sleep(pace)

            if upcoming:
                data = await upcoming
            elif token:
                data = await fetch(token)
            else:
                return
    finally:
        if upcoming and not upcoming.done():
            upcoming.cancel()


async def spread[T](work: Callable[[T], Awaitable[object]], items: Collection[T], width: int = CONCURRENCY):
    """Run `work` over every item, keeping at most `width` calls in flight.

    Handing the whole backlog to `gather` instead parks thousands of waiters on the shared
    semaphore, and that queue is FIFO: every other collection then sits behind them, dead
    still, until this one drains. Bounding the queue costs no throughput -- the semaphore
    was always the real ceiling -- it only stops one collection from owning all of it.
    """
    todo = iter(items)

    async def worker():
        for item in todo:  # one shared iterator: whoever finishes first takes the next item
            await work(item)

    await gather(*(worker() for _ in range(min(width, len(items)))))
