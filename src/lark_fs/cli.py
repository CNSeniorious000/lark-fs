"""Thin async wrapper around the `lark-cli` binary."""

from asyncio import CancelledError, Semaphore, create_subprocess_exec, create_task, shield, sleep, subprocess
from collections import defaultdict, deque
from contextlib import suppress
from contextvars import ContextVar
from itertools import count, pairwise
from json import JSONDecoder
from re import compile
from typing import Any

CONCURRENCY = 3  # lark's open API rate-limits aggressively; keep this low
_sem = Semaphore(CONCURRENCY)

FEED_LIMIT = 10  # how many recent requests the TUI shows


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

    def finish(self, rid: int, state: str):
        if entry := self.running.pop(rid, None):
            group, domain, subject = entry
            self.done[group].append((domain, subject, state))

    def rows(self, group: str, limit: int) -> list[tuple[str, str, str]]:
        live = [(d, s, "running") for g, d, s in self.running.values() if g == group]
        return (list(self.done.get(group, ())) + live)[-limit:]

    def busy(self, group: str) -> int:
        return sum(1 for g, _, _ in self.running.values() if g == group)


activity = Activity()
current_group: ContextVar[str] = ContextVar("current_group", default="")  # which collection issued this request
_next_id = count()


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


async def run(*argv: str, retries: int = 5, cwd: str | None = None) -> Any:
    """Run a lark-cli command expecting JSON on stdout. Returns the `data` field."""
    Aborted.check()
    args = ["lark-cli", *argv, "--format", "json"]
    delay = 2.0
    for attempt in range(retries):
        async with _sem:
            rid = next(_next_id)
            activity.start(rid, current_group.get(), *_label(argv))
            failed = True
            try:
                proc = await create_subprocess_exec(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
                out, stderr = await proc.communicate()
                failed = False
            except CancelledError:
                # ctrl-c: kill the child and *wait for it*. Without the wait, asyncio reaps the
                # subprocess transport after the loop has closed and complains "Loop ... is closed".
                with suppress(ProcessLookupError, UnboundLocalError):
                    proc.kill()
                    with suppress(CancelledError):
                        await shield(proc.wait())
                raise
            finally:
                activity.finish(rid, "error" if failed else "done")
        payload = _parse(out.decode(errors="replace"))
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
