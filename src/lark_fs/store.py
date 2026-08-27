"""On-disk layout + incremental-sync bookkeeping.

Everything is one entity per file, named by its Lark ID, so `fd om_xxx` and
`rg 'oc_xxx'` are the primary query interface. Cross-entity references are
always raw IDs, never paths -- so a grep for an ID finds every mention of it.

Structured data is rendered as readable YAML (unquoted scalars, literal blocks
for multi-line text) rather than JSON, so message bodies stay greppable as
plain lines instead of being buried in \n escapes.

    <root>/
      .lark-fs/cursors.yaml      # per-collection incremental cursors
      chats/<chat_id>/
        meta.yaml
        members.yaml
        media.yaml               # image/file keys seen in this chat; the files land under files/
        messages/<YYYY-MM>/<message_id>.yaml
        threads/<thread_id>/<message_id>.yaml
        files/<file_key>/<original name>    # only for kinds enabled in .lark-fs/config.toml
      users/<open_id>/meta.yaml
      docs/<doc_token>/{meta.yaml,content.md,comments.yaml}
      minutes/<minute_token>/{meta.yaml,transcript.txt,summary.md,chapters.yaml,todos.yaml}
      meetings/<meeting_id>/meta.yaml
      bases/<app_token>/tables/<table_id>/{meta.yaml,records.yaml}
      wiki/<space_id>/{meta.yaml,nodes.yaml}
"""

from json import dumps, loads
from pathlib import Path
from typing import Any

from yaml import YAMLError, safe_load

from .yaml import readable_yaml_dumps


class Store:
    def __init__(self, root: Path):
        self.root = root
        self.meta_dir = root / ".lark-fs"
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self._cursors_path = self.meta_dir / "cursors.json"
        self.cursors: dict[str, Any] = loads(self._cursors_path.read_text()) if self._cursors_path.exists() else {}

    def save_cursors(self):
        self._cursors_path.write_text(dumps(self.cursors, ensure_ascii=False, indent=2))

    def save_cursor(self, key: str):
        """Persist one key without carrying the rest of this Store's snapshot with it.

        Cursors are read into memory once, at construction, and `save_cursors` writes the
        whole dict back -- fine for the sync, which owns the file for its run, and wrong
        for anything holding a Store alongside it. The daemon keeps one from startup and
        hands it to `recheck_messages` while `sync_all` runs on its own; `reindex` makes a
        third and takes minutes. Whichever writes last erases every advance the others
        made: chat cursors regress to a stale snapshot, and `threads_incomplete`
        registrations disappear -- 57 threads on this store sat truncated at the inline cap
        with nothing queued to finish them.

        Merging instead of replacing would not do: the repair queue has to be able to
        shrink, and a merge would restore every entry a repair had just cleared.
        """
        on_disk = loads(self._cursors_path.read_text()) if self._cursors_path.exists() else {}
        on_disk[key] = self.cursors.get(key)
        self._cursors_path.write_text(dumps(on_disk, ensure_ascii=False, indent=2))

    def write(self, rel: str, content: str) -> bool:
        """Write text, returning whether it actually changed (keeps mtimes meaningful)."""
        path = self.root / rel
        if path.exists() and path.read_text() == content:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return True

    def write_yaml(self, rel: str, obj: Any) -> bool:
        return self.write(rel, readable_yaml_dumps(obj))

    def append_yaml(self, rel: str, rows: list[Any]):
        """Append list items to a YAML sequence file (used for record/node collections)."""
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as f:
            f.write(readable_yaml_dumps(rows))

    def read_yaml_rows(self, rel: str) -> list[dict]:
        """Read back a YAML sequence this store wrote."""
        path = self.root / rel
        return _rows(path.read_text()) if path.exists() else []

    def glob_rows(self, pattern: str) -> list[dict]:
        """Every row of every sequence matching `pattern`, merged.

        Node lists and media indexes are one file per space or chat, and every reader of
        them wants the union -- which each row already carries enough ids to attribute.
        """
        return [row for f in self.root.glob(pattern) for row in _rows(f.read_text())]

    def read_yaml(self, rel: str) -> dict:
        """Load back a mapping this store wrote. Returns {} when absent or unparseable."""
        path = self.root / rel
        if not path.exists():
            return {}
        try:
            return safe_load(path.read_text()) or {}
        except YAMLError:
            return {}

    def exists(self, rel: str) -> bool:
        return (self.root / rel).exists()

    def count(self, pattern: str) -> int:
        return sum(1 for _ in self.root.glob(pattern))


def _unquote(value: str) -> str:
    """Undo whatever `_serialize_scalar` quoted this with.

    Stripping the quote characters is not the same thing. A single-quoted scalar doubles its
    own quote, and a double-quoted one is the only style carrying escapes -- so a real
    attachment named `C'est l'application ....mp4` came back with its double quotes still
    attached and became part of the filename, and a backslash came back wrong. The parser
    is only paid for on a value that was quoted, which in a 40k-row index is a handful.
    """
    if value.startswith('"'):
        return safe_load(value)
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1].replace("''", "'")
    return value


def _rows(text: str) -> list[dict]:
    """Parse the flat `- key: value` shape the sequence writers emit -- enough to merge
    across runs without a YAML parser, which on a 40k-row media index is the difference
    between milliseconds and seconds."""
    rows: list[dict] = []
    for line in text.splitlines():
        if line.startswith("- "):
            rows.append({})
            line = line[2:]
        elif line.startswith("  "):
            line = line[2:]
        else:
            continue
        key, _, value = line.partition(": ")
        if rows and key:
            rows[-1][key] = _unquote(value.strip())
    return rows
