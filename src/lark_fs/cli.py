"""Thin async wrapper around the `lark-cli` binary."""

from asyncio import CancelledError, Semaphore, create_subprocess_exec, create_task, gather, shield, sleep, subprocess
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Collection
from contextlib import suppress
from contextvars import ContextVar
from html import unescape
from itertools import count, pairwise
from json import JSONDecoder
from os import environ
from re import compile
from typing import Any

# The search endpoints rate-limit hard (that is what forced this down to 3), but the
# per-chat listing does not: 40 chats at 8-way concurrency sustained 88 msg/s with zero
# 429s, against 10 msg/s before. Raise this only with a measurement, never a guess.
CONCURRENCY = 8
_sem = Semaphore(CONCURRENCY)

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
    if start := dict(pairwise(argv)).get("--start"):
        return domain, f"{verb} {start[:10]}"
    ids = [v for f, v in pairwise(argv) if f.startswith("--") and RE_ID.fullmatch(v)]
    return domain, f"{verb} {ids[-1]}".strip() if ids else verb


RE_ID = compile(r"(?=[A-Za-z0-9_-]*\d)[A-Za-z0-9_-]{8,}")
RE_MARKUP = compile(r"</?[a-zA-Z][^>]*>")
RE_PAGE_CAP = compile(r"--page-size \d+: must be between 1 and (\d+)")
RE_DATE = compile(r"\d{4}-\d{2}-\d{2}")
RE_CODE = compile(r'"code":\s*(\d+)')


class Aborted:
    """Cooperative stop. Every request is a checkpoint, so a sync interrupts promptly
    no matter which collection is running."""

    flag = False
    reason = ""  # why, when it was not a keystroke -- a stop the user cannot act on the same way

    @classmethod
    def check(cls):
        if cls.flag:
            raise SyncAbortedError


class SyncAbortedError(Exception):
    """Raised at a request boundary once a stop has been requested."""


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
        """1069307: this endpoint will never have anything for that token.

        Permanent, unlike a rate limit -- worth remembering, so the same request is not
        reissued on every run.
        """
        return self._code == 1069307

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
    def is_forbidden(self):
        """The account can see that this exists and cannot read it.

        One thing, a different shape per endpoint: `docs +fetch` answers 3380004, `drive
        +list-comments` 131006, `note +detail` 121005, and `minutes +detail` says so in the
        body per token rather than as a code, offering to ask the owner -- which this will
        not do on anyone's behalf. 189 of the 710 minutes the meetings name are in this
        state, and they were 189 requests every sync.

        Unlike `is_missing` this can stop being true, so it is remembered as a clock.
        """
        return self._code in (3380004, 131006, 121005) or "No read permission" in str(self.payload)

    @property
    def is_quota_exhausted(self):
        """99991403: the tenant's *monthly* API allowance is gone.

        A different ceiling from 99991400 and a far worse one -- it counts every custom
        app in the tenant together, resets on the 1st, and no amount of backoff clears it.
        Retrying is not just useless, it burns the next month's budget once it rolls over.
        """
        return self._code == 99991403


def _parse(text: str) -> Any | None:
    """Extract lark-cli's JSON body.

    Some shortcuts print progress lines before it and trailing output after it, so decode
    the first complete object rather than requiring the whole tail to be valid JSON --
    otherwise a truncated trailer turns a structured API error into an opaque blob.
    """
    for i, ch in enumerate(text):
        if ch == "{":
            try:
                return JSONDecoder().raw_decode(text, i)[0]
            except ValueError:
                continue
    return None


async def run(*argv: str, retries: int = 5, cwd: str | None = None, subject: str = "") -> Any:
    """Run a lark-cli command expecting JSON on stdout. Returns the `data` field."""
    Aborted.check()
    args = ["lark-cli", *argv, "--format", "json"]
    delay = 2.0
    for attempt in range(retries):
        async with _sem:
            rid = next(_next_id)
            if feed_enabled:
                domain, guessed = _label(argv)
                activity.start(rid, current_group.get(), domain, subject or guessed)
            failed = True
            outcome = ""
            payload = None
            try:
                proc = await create_subprocess_exec(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
                try:
                    out, stderr = await proc.communicate()
                except CancelledError:
                    # ctrl-c: kill the child and *wait for it*. Without the wait, asyncio reaps the
                    # subprocess transport after the loop has closed and complains "Loop ... is closed".
                    with suppress(ProcessLookupError):
                        proc.kill()
                        with suppress(CancelledError):
                            await shield(proc.wait())
                    raise
                failed = False
                payload = _parse(out.decode(errors="replace"))
                outcome = "" if subject else (_summarise(payload) if feed_enabled else "")
            finally:
                if feed_enabled:
                    activity.finish(rid, "error" if failed else "done", outcome)
        if payload is None:
            raise LarkError(list(argv), {"error": {"message": (out.decode(errors="replace") or stderr.decode(errors="replace") or f"exit {proc.returncode}")[:400]}})
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
    raise AssertionError("unreachable")


async def paginate(*argv: str, key: str, page_size: int = 50, prefetch: bool = False):
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
