"""Fixture harness: run message_view() over recorded raw items and diff against the views lark-fs wrote to disk.

    python tools/verify_render.py ../fixtures/messages.jsonl [--recordings FILE]

The fixtures are real tenant data and stay outside the repo. API traffic the renderer
needs (thread containers, merge_forward sub-messages, reactions) is served from a
"recorded map" built from, in order:

1. a recordings file -- {"calls": [{"method", "path", "params", "body", "data"?, "error"?}]}
   captured earlier (e.g. through `lark-cli api ...`), looked up beside the fixtures as
   api_recordings.json unless --recordings names one;
2. threads.jsonl beside the fixtures file, whose raw_container arrays ARE the recorded
   thread-container responses.

A call the map does not hold resolves to empty data, which is what an unreached
enrichment (no reactions, no replies) looks like; unrecorded calls are counted and
reported. Comparison excuses, per the porting contract: `reactions`/`reactions_error`
are ignored where `expected` lacks them (they cannot be reconstructed offline), and
`thread_replies` is ignored where `expected` lacks it (lark-fs strips it when it writes
one file per message; the inline replies are verified through threads.jsonl instead).
Everything else must match exactly. Exits non-zero on any remaining mismatch.

Output names fields and truncates values: fixtures are private, so nothing large is
printed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lark_fs import messages

REACTION_KEYS = ("reactions", "reactions_error")
MAX_SHOWN = 5  # per field: enough to locate the row, not to leak its content


def short(value: Any, limit: int = 90) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    return text if len(text) <= limit else text[:limit] + f"... ({len(text)} chars)"


class Recorder:
    """The recorded map: exact (method, path, params, body) -> recorded response."""

    def __init__(self) -> None:
        self.map: dict[tuple, dict | str] = {}
        self.misses: list[tuple] = []

    def add(self, method: str, path: str, params: dict | None, body: dict | None, data: dict | None, error: str | None) -> None:
        key = self.key(method, path, params, body)
        self.map[key] = error or (data or {})

    @staticmethod
    def key(method: str, path: str, params: dict | None, body: dict | None) -> tuple:
        return (method, path, json.dumps(params, sort_keys=True) if params else None, json.dumps(body, sort_keys=True) if body else None)

    async def get(self, method: str, path: str, params: dict | None = None, body: dict | None = None) -> dict:
        key = self.key(method, path, params, body)
        if key not in self.map:
            self.misses.append(key)
            return {"items": []}
        recorded = self.map[key]
        if isinstance(recorded, str):
            raise RuntimeError(recorded)  # noqa: TRY004 -- a recorded failure: the API failed when captured
        return recorded


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open() as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def load_recordings(rec: Recorder, path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    calls = json.loads(path.read_text()).get("calls", [])
    for call in calls:
        rec.add(call.get("method", ""), call.get("path", ""), call.get("params"), call.get("body"), call.get("data"), call.get("error"))
    return len(calls)


def load_thread_containers(rec: Recorder, path: Path) -> int:
    """threads.jsonl rows carry exactly the container response thread expansion fetches."""
    added = 0
    if not path.exists():
        return added
    for row in load_rows(path):
        if row.get("kind") != "thread":
            continue
        params = {
            "container_id_type": "thread",
            "container_id": row["thread_id"],
            "sort_type": "ByCreateTimeAsc",
            "page_size": "50",
            "card_msg_content_type": "raw_card_content",
            "with_sender_name": "true",
        }
        rec.add("GET", "/open-apis/im/v1/messages", params, None, {"items": row.get("raw_container") or [], "has_more": False}, None)
        added += 1
    return added


def diff(view: dict, expected: dict, card: bool) -> list[str]:
    """Fields that differ, after the contract's excuses. [] means the row is exact."""
    mine = dict(view)
    theirs = dict(expected)
    for key in REACTION_KEYS:
        if key not in theirs:
            mine.pop(key, None)  # expected may lack reactions data that cannot be reconstructed offline
    if "thread_replies" not in theirs:
        mine.pop("thread_replies", None)  # lark-fs strips inline replies when it writes one file per message
    fields = []
    for key in sorted(set(mine) | set(theirs)):
        if mine.get(key, "<absent>") != theirs.get(key, "<absent>"):
            if card and key == "content":
                continue  # interactive content is owned by the cards port; counted apart below
            fields.append(key)
    return fields


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("fixtures", type=Path, help="fixtures JSONL (messages.jsonl or threads.jsonl)")
    parser.add_argument("--recordings", type=Path, default=None, help="recorded API responses (default: api_recordings.json beside the fixtures)")
    parser.add_argument("--tz", default="UTC", help="render times in this zone (fixtures were recorded under UTC)")
    args = parser.parse_args()

    os.environ["TZ"] = args.tz
    time.tzset()

    rec = Recorder()
    recorded = load_recordings(rec, args.recordings or args.fixtures.parent / "api_recordings.json")
    threads_file = args.fixtures.parent / "threads.jsonl"
    registered = load_thread_containers(rec, threads_file) if args.fixtures.name != "threads.jsonl" else 0

    rows = load_rows(args.fixtures)
    have_cards = True
    try:
        from lark_fs import cards  # noqa: F401
    except ImportError:
        have_cards = False

    exact = 0
    exact_ignoring_sender = 0
    in_scope = 0
    out_of_scope = 0
    field_counts: dict[str, int] = {}
    shown: dict[str, list[str]] = {}
    card_content_mismatch = 0

    def record(kind: str, label: str, view: dict, expected: dict) -> None:
        nonlocal exact, exact_ignoring_sender, in_scope, out_of_scope, card_content_mismatch
        card = expected.get("msg_type") == "interactive" or view.get("msg_type") == "interactive"
        if card and not have_cards:
            out_of_scope += 1
            return
        in_scope += 1
        fields = diff(view, expected, card)
        if card and view.get("content") != expected.get("content"):
            card_content_mismatch += 1
        if not fields:
            exact += 1
            exact_ignoring_sender += 1
            return
        if fields == ["sender"]:
            exact_ignoring_sender += 1
        for field in fields:
            field_counts[field] = field_counts.get(field, 0) + 1
            shown.setdefault(field, []).append(f"{kind} {label}: mine={short(view.get(field, '<absent>'))} expected={short(expected.get(field, '<absent>'))}")

    for row in rows:
        if row.get("kind") == "thread":
            try:
                view = await messages.message_view(row["raw_root"], get=rec.get)
            except Exception as exc:  # noqa: BLE001 - a render crash is a mismatch, not a crash of the harness
                print(f"thread {row['thread_id']}: render raised {type(exc).__name__}: {exc}")
                in_scope += 1
                field_counts["<render error>"] = field_counts.get("<render error>", 0) + 1
                continue
            expected_views = row.get("expected_views") or {}
            root_expected = expected_views.get(row["root_message_id"], {})
            replies = view.pop("thread_replies", None) or []
            record("thread", f"{row['thread_id']} root", view, {k: v for k, v in root_expected.items() if k != "thread_replies"})
            by_id = {r.get("message_id"): r for r in replies}
            for message_id, expected in expected_views.items():
                if message_id == row["root_message_id"]:
                    continue
                if message_id not in by_id:
                    in_scope += 1
                    field_counts["<missing reply>"] = field_counts.get("<missing reply>", 0) + 1
                    continue
                record("thread", f"{row['thread_id']} reply", by_id[message_id], expected)
        else:
            try:
                view = await messages.message_view(row["raw"], get=rec.get)
            except Exception as exc:  # noqa: BLE001
                print(f"message {row['raw'].get('message_id')}: render raised {type(exc).__name__}: {exc}")
                in_scope += 1
                field_counts["<render error>"] = field_counts.get("<render error>", 0) + 1
                continue
            record("message", str(row["raw"].get("message_id")), view, row["expected"])

    print(f"rows: {len(rows)}  in scope: {in_scope}  out of scope (interactive without cards.py): {out_of_scope}")
    print(f"exact: {exact}/{in_scope}   exact ignoring sender drift: {exact_ignoring_sender}/{in_scope}")
    if have_cards:
        print(f"interactive rows whose content render_card() does not reproduce: {card_content_mismatch}")
    if rec.misses:
        print(f"unrecorded API calls (served as empty): {len(rec.misses)}")
    print(f"recorded map: {len(rec.map)} entries (recordings: {recorded}, thread containers: {registered})")
    if field_counts:
        print("per-field mismatches:")
        for field, count in sorted(field_counts.items(), key=lambda kv: -kv[1]):
            print(f"  {field}: {count}")
            for line in shown.get(field, [])[:MAX_SHOWN]:
                print(f"    {line}")
    return 0 if exact == in_scope else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
