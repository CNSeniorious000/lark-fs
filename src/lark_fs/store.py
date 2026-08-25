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
        media.yaml               # image/file keys seen in this chat; bytes are never downloaded
        messages/<YYYY-MM>/<message_id>.yaml
        threads/<thread_id>/<message_id>.yaml
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
        """Read back a YAML sequence this store wrote. Only handles the flat `- key: value`
        shape used by the media index -- enough to merge across runs without a YAML parser."""
        path = self.root / rel
        if not path.exists():
            return []
        rows: list[dict] = []
        for line in path.read_text().splitlines():
            if line.startswith("- "):
                rows.append({})
                line = line[2:]
            elif line.startswith("  "):
                line = line[2:]
            else:
                continue
            key, _, value = line.partition(": ")
            if rows and key:
                rows[-1][key] = value.strip().strip("'")
        return rows

    def exists(self, rel: str) -> bool:
        return (self.root / rel).exists()

    def count(self, pattern: str) -> int:
        return sum(1 for _ in self.root.glob(pattern))
