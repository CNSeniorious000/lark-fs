"""Rebuild derived indexes from messages already on disk, without touching the network.

`users/` and each chat's `media.yaml` are both projections of the message files, so a
store synced before those projections existed (or after their extraction rules changed)
can be brought up to date locally instead of re-fetching under a rate limit.
"""

from pathlib import Path

from yaml import YAMLError, safe_load

from .store import Store
from .sync import _flush_media, _index_media, _record_sender, migrate_threads


def reindex(root: Path) -> dict[str, int]:
    store = Store(root)
    split = migrate_threads(store)  # before the scan, so the messages it frees are indexed by it
    known: set[str] = set()
    media: list[dict] = []
    scanned = 0
    unreadable: list[Path] = []

    for path in sorted((root / "chats").glob("*/*/*/*.yaml")):
        try:
            msg = safe_load(path.read_text())
        except YAMLError:
            # Written by a serializer that could not round-trip what the message contained.
            # The file is on disk but says nothing readable, and only the API still has the
            # real content -- so record the id for the next sync to fetch and rewrite.
            unreadable.append(path)
            store.cursors.setdefault("messages_unreadable", {})[path.stem] = str(path.relative_to(root))
            continue
        if not isinstance(msg, dict) or not msg.get("message_id"):
            continue
        scanned += 1
        media += _index_media(msg)
        _record_sender(store, msg, known)

    _flush_media(store, media)
    # this scan takes minutes; a full save would write back the snapshot it loaded at the
    # start, undoing every cursor a sync advanced while it ran
    store.save_cursor("messages_unreadable")
    return {"messages": scanned, "users": len(known), "media": len(media), "unreadable": len(unreadable), "threads_split": split, "damaged": len(_damaged(root))}


def _damaged(root: Path) -> list[Path]:
    """Every file in the store this mirror can no longer read back.

    The loop above only reaches messages, because that is what it rebuilds projections
    from. Nothing looked at the rest -- and five `docs/*/comments.yaml`, written by a
    serializer that could not round-trip a block scalar whose first line was blank and
    more indented than its content, sat unreadable with nothing to say so. A message has
    `messages_unreadable` to get it refetched; these had no equivalent, so the only
    protection is noticing.
    """
    out = []
    for f in root.rglob("*.yaml"):
        try:
            safe_load(f.read_text())
        except YAMLError, UnicodeDecodeError:
            out.append(f)
    return out
