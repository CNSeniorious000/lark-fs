"""Thin async wrapper around the `lark-cli` binary."""

from asyncio import CancelledError, Semaphore, create_subprocess_exec, create_task, sleep, subprocess
from contextlib import suppress
from itertools import count, pairwise
from json import JSONDecoder
from re import compile
from typing import Any

CONCURRENCY = 3  # lark's open API rate-limits aggressively; keep this low
_sem = Semaphore(CONCURRENCY)

in_flight: dict[int, str] = {}  # request id -> label, so the TUI can show what is being fetched
_next_id = count()


def _label(argv: tuple[str, ...]) -> str:
    """`("minutes", "+detail", "--minute-tokens", "obc...")` -> `minutes +detail obc...`

    The interesting part is the first flag *value* -- an id or token -- so pair each
    `--flag` with what follows it and take the first one that looks like an argument.
    """
    head = " ".join(argv[:2])
    ids = (v for f, v in pairwise(argv) if f.startswith("--") and not v.startswith("--"))
    subject = next((v for v in ids if not v.isdigit() or len(v) > 8), "")
    return f"{head} {subject}".strip()


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
            in_flight[rid] = _label(argv)
            try:
                proc = await create_subprocess_exec(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
                out, stderr = await proc.communicate()
            except CancelledError:
                # ctrl-c: kill the child instead of letting its pending read surface as an unhandled error
                with suppress(ProcessLookupError, UnboundLocalError):
                    proc.kill()
                raise
            finally:
                del in_flight[rid]
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
    token = None
    data = None
    while True:
        if data is None:
            extra = ["--page-size", str(page_size), *(["--page-token", token] if token else [])]
            try:
                data = await run(*argv, *extra)
            except LarkError as e:
                if e.is_pagination_exhausted:
                    return
                raise
        items = data.get(key) or []
        token = data.get("page_token")
        more = bool(token and data.get("has_more"))

        if prefetch and more:
            # kick off the next request first, then hand out this page while it flies
            extra = ["--page-size", str(page_size), "--page-token", token]
            upcoming = create_task(run(*argv, *extra))
            pace = 1.0 / (len(items) or 1)  # spread this page across roughly one request
            for item in items:
                yield item
                await sleep(pace)
            try:
                data = await upcoming
            except LarkError as e:
                if e.is_pagination_exhausted:
                    return
                raise
            continue

        for item in items:
            yield item
        if not more:
            return
