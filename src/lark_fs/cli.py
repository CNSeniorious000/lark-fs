"""Thin async wrapper around the `lark-cli` binary."""

from asyncio import Semaphore, create_subprocess_exec, sleep, subprocess
from itertools import count, pairwise
from json import loads
from typing import Any

CONCURRENCY = 4  # lark's open API rate-limits aggressively; keep this low
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


class LarkError(Exception):
    def __init__(self, argv: list[str], payload: Any):
        self.argv, self.payload = argv, payload
        super().__init__(f"{' '.join(argv)} -> {payload}")

    @property
    def missing_scopes(self) -> list[str]:
        return (self.payload or {}).get("error", {}).get("missing_scopes", []) if isinstance(self.payload, dict) else []

    @property
    def is_rate_limited(self):
        if not isinstance(self.payload, dict):
            return False
        err = self.payload.get("error", {})
        # 99991400 is Lark's "request trigger frequency limit"; the subtype spelling varies by endpoint
        return err.get("code") == 99991400 or "rate_limit" in str(err.get("subtype", ""))


def _parse(text: str) -> Any | None:
    """lark-cli prefixes some shortcuts with progress lines; the JSON body starts at a bare `{`."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("{"):
            try:
                return loads("\n".join(lines[i:]))
            except ValueError:
                continue
    return None


async def run(*argv: str, retries: int = 5, cwd: str | None = None) -> Any:
    """Run a lark-cli command expecting JSON on stdout. Returns the `data` field."""
    args = ["lark-cli", *argv, "--format", "json"]
    delay = 2.0
    for attempt in range(retries):
        async with _sem:
            rid = next(_next_id)
            in_flight[rid] = _label(argv)
            try:
                proc = await create_subprocess_exec(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
                out, stderr = await proc.communicate()
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


async def paginate(*argv: str, key: str, page_size: int = 20):
    """Yield items across pages for shortcuts exposing --page-token/--page-size."""
    token = None
    while True:
        extra = ["--page-size", str(page_size), *(["--page-token", token] if token else [])]
        data = await run(*argv, *extra)
        for item in data.get(key) or []:
            yield item
        token = data.get("page_token")
        if not token or not data.get("has_more"):
            return
