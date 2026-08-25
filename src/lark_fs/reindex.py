"""Rebuild derived indexes from messages already on disk, without touching the network.

`users/` and each chat's `media.yaml` are both projections of the message files, so a
store synced before those projections existed (or after their extraction rules changed)
can be brought up to date locally instead of re-fetching under a rate limit.
"""

from pathlib import Path

from yaml import YAMLError, safe_load

from .store import Store
from .sync import _flush_media, _index_media, _record_sender


def reindex(root: Path) -> dict[str, int]:
    store = Store(root)
    known: set[str] = set()
    media: list[dict] = []
    scanned = 0
    unreadable: list[Path] = []

    for path in sorted((root / "chats").glob("*/*/*/*.yaml")):
        try:
            msg = safe_load(path.read_text())
        except YAMLError:
            # written before control characters were stripped; re-syncing that slice fixes it
            unreadable.append(path)
            continue
        if not isinstance(msg, dict) or not msg.get("message_id"):
            continue
        scanned += 1
        media += _index_media(msg)
        _record_sender(store, msg, known)

    _flush_media(store, media)
    return {"messages": scanned, "users": len(known), "media": len(media), "unreadable": len(unreadable)}
