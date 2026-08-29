"""Rebuild derived indexes from messages already on disk, without touching the network.

`users/` and each chat's `media.yaml` are both projections of the message files, so a
store synced before those projections existed (or after their extraction rules changed)
can be brought up to date locally instead of re-fetching under a rate limit.
"""

from pathlib import Path

from yaml import YAMLError, safe_load

from .store import Store
from .sync import _doc_links, _flush_doc_links, _flush_media, _index_media, _record_sender, migrate_meetings, migrate_minutes, migrate_threads


def reindex(root: Path) -> dict[str, int]:
    store = Store(root)
    split = migrate_threads(store)  # before the scan, so the messages it frees are indexed by it
    migrate_meetings(store)
    migrate_minutes(store)
    known: set[str] = set()
    media: list[dict] = []
    links: list[tuple[str, str]] = []
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
        links += _doc_links(str(msg.get("content") or ""))
        _record_sender(store, msg, known)

    _flush_media(store, media)
    # the backlog: a link only reaches the doc pass once something has read the text carrying
    # it, and the doc pass reads a body once and then leaves it alone for as long as it is current
    links += [link for f in (root / "docs").glob("*/content.md") for link in _doc_links(f.read_text())]
    _flush_doc_links(store, links)
    # this scan takes minutes; a full save would write back the snapshot it loaded at the
    # start, undoing every cursor a sync advanced while it ran
    store.save_cursor("messages_unreadable")
    return {"messages": scanned, "users": len(known), "media": len(media), "doc_links": len(links), "unreadable": len(unreadable), "threads_split": split, "damaged": len(_damaged(root))}


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
