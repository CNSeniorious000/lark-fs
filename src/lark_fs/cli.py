"""Thin async wrapper around the `lark-cli` binary."""

from asyncio import CancelledError, Semaphore, create_subprocess_exec, create_task, shield, sleep, subprocess
from collections import defaultdict, deque
from contextlib import suppress
from contextvars import ContextVar
from html import unescape
from itertools import count, pairwise
from json import JSONDecoder
from re import compile
from typing import Any

CONCURRENCY = 3  # lark's open API rate-limits aggressively; keep this low
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
                return "empty"  # an empty window is a result too; saying so beats echoing the query
            head = _title(items[0])
            label = "" if key == "items" else f" {key}"
            return f"{len(items)}{label}" + (f" · {head}" if head else "")
    return _title(data)


def oneline(text, limit: int = 60) -> str:
    """Collapse to a single line and bound its length, for progress display."""
    flat = " ".join(str(text or "").split())
    return flat[: limit - 1] + "…" if len(flat) > limit else flat


def _title(item) -> str:
    if not isinstance(item, dict):
        return ""
    for key in TITLE_KEYS:
        if isinstance(v := item.get(key), str) and v.strip():
            return oneline(unescape_entities(v.replace("<h>", "").replace("</h>", "")), 48)
    if isinstance(nested := item.get("result_meta"), dict):
        return _title(nested)  # drive search nests the fields one level down
    return ""


RE_ENTITY = compile(r"&(#\d+|#[xX][0-9a-fA-F]+|\w+);")


def unescape_entities(text: str) -> str:
    """Decode only the semicolon form. `html.unescape` also decodes entities written
    without one, so a URL carrying `&timestamp=` or `&notify=` silently loses characters.
    """
    return RE_ENTITY.sub(lambda m: unescape(m[0]), text)


TENANT = "https://acme.feishu.cn"


def link_for(token: str) -> str:
    """Best-effort Feishu URL for an opaque token, so a feed line can be clicked.

    Prefixes are the only signal available -- the CLI takes bare tokens and never tells us
    what kind of object they name -- but they are stable and unambiguous in practice.
    """
    if token.startswith("oc_"):
        return f"https://applink.feishu.cn/client/chat/open?openChatId={token}"
    if token.startswith("ou_"):
        return f"https://applink.feishu.cn/client/contact/open?openId={token}"
    if token.startswith("obc"):
        return f"{TENANT}/minutes/{token}"
    if token.startswith("tbl") or token.startswith("bas"):
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
    verb = argv[1].lstrip("+") if len(argv) > 1 else ""
    flags = dict(pairwise(argv))
    # a time-windowed sweep is better identified by its window than by any token in it
    if start := flags.get("--start"):
        return argv[0], f"{verb} {start[:10]}"
    ids = [v for f, v in pairwise(argv) if f.startswith("--") and RE_ID.fullmatch(v)]
    return argv[0], f"{verb} {ids[-1]}".strip() if ids else verb


RE_ID = compile(r"(?=[A-Za-z0-9_-]*\d)[A-Za-z0-9_-]{8,}")
RE_DATE = compile(r"\d{4}-\d{2}-\d{2}")
RE_CODE = compile(r'"code":\s*(\d+)')


class Aborted:
    """Cooperative stop. Every request is a checkpoint, so a sync interrupts promptly
    no matter which collection is running."""

    flag = False

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
    def is_pagination_exhausted(self):
        """121022: the endpoint refuses to page further. Expected on wide windows --
        it means "that's all you get here", not that the collection failed."""
        return self._code == 121022

    @property
    def is_rate_limited(self):
        # 99991400 is Lark's "request trigger frequency limit"; the subtype spelling varies by endpoint
        return self._code == 99991400 or "rate_limit" in str(self.payload)


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
                out, stderr = await proc.communicate()
                failed = False
                payload = _parse(out.decode(errors="replace"))
                outcome = "" if subject else (_summarise(payload) if feed_enabled else "")
            except CancelledError:
                # ctrl-c: kill the child and *wait for it*. Without the wait, asyncio reaps the
                # subprocess transport after the loop has closed and complains "Loop ... is closed".
                with suppress(ProcessLookupError, UnboundLocalError):
                    proc.kill()
                    with suppress(CancelledError):
                        await shield(proc.wait())
                raise
            finally:
                if feed_enabled:
                    activity.finish(rid, "error" if failed else "done", outcome)
        if payload is None:
            raise LarkError(list(argv), {"error": {"message": (out.decode(errors="replace") or stderr.decode(errors="replace") or f"exit {proc.returncode}")[:400]}})
        if payload.get("ok"):
            return payload.get("data")
        exc = LarkError(list(argv), payload)
        if not exc.is_rate_limited or attempt == retries - 1:
            raise exc
        await sleep(delay)
        delay *= 2
    raise AssertionError("unreachable")


async def paginate(*argv: str, key: str, page_size: int = 20, prefetch: bool = False):
    """Yield items across pages for shortcuts exposing --page-token/--page-size.

    With `prefetch`, the next page is requested while the current one is still being
    consumed. A page arrives in milliseconds but the next request takes a second or
    two, so without this the caller does all its work in one burst and then idles --
    which reads as a frozen counter rather than steady progress.
    """

    async def fetch(token: str | None):
        try:
            return await run(*argv, "--page-size", str(page_size), *(["--page-token", token] if token else []))
        except LarkError as e:
            if e.is_pagination_exhausted:
                return None
            raise

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
