"""Keep the mirror fresh in the background.

Feishu offers no update feed a personal account can consume -- the event bus is
bot-scoped and exclusive, and one connection is typically already claimed by another
tool. So this polls, but pays for freshness only where it is cheap:

- messages: forward from the cursor every cycle, then replay recent ids to catch
  edits and recalls (which leave no forward trace) at a slower cadence
- docs: the server reports an authoritative update_time, so a sweep is cheap
- everything else: the archive-shaped collections, refreshed occasionally
"""

from asyncio import sleep
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from . import cli
from .store import Store
from .sync import Progress, recheck_messages, sync_all


@dataclass(frozen=True)
class Schedule:
    """How often each tier runs, in seconds."""

    messages: float = 120
    recheck: float = 1800
    archive: float = 3600


async def watch(root: Path, p: Progress, schedule: Schedule | None = None, cycles: int = 0):
    """Poll forever (or `cycles` times), running each tier when it comes due."""
    schedule = schedule or Schedule()
    store = Store(root)
    due = dict.fromkeys(("messages", "recheck", "archive"), 0.0)
    ran = 0

    while not cycles or ran < cycles:
        now = datetime.now(UTC).timestamp()
        tiers = [name for name, at in due.items() if now >= at]

        if "archive" in tiers:
            await sync_all(root, p)
            due["archive"] = now + schedule.archive
            due["messages"] = now + schedule.messages  # sync_all already advanced messages
        elif "messages" in tiers:
            await sync_all(root, p, ["messages"])
            due["messages"] = now + schedule.messages

        if "recheck" in tiers:
            await recheck_messages(store, p)
            due["recheck"] = now + schedule.recheck

        store.save_cursors()
        ran += 1
        if cycles and ran >= cycles:
            return store

        p.set("daemon", state="running", note=f"next sweep in {int(min(due.values()) - datetime.now(UTC).timestamp())}s")
        await _idle_until(min(due.values()))


async def _idle_until(deadline: float):
    """Sleep in short hops so a stop request is honoured promptly."""
    while (left := deadline - datetime.now(UTC).timestamp()) > 0:
        cli.Aborted.check()
        await sleep(min(left, 1.0))
