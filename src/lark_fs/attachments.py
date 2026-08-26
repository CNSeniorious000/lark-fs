"""Fetching the attachments themselves, so a text one is greppable like any other file.

Lark never reports an attachment's size until it has been downloaded -- the message body
carries only a key and a filename -- so the size limit is enforced after the fact and the
kind whitelist is what actually bounds the request count.
"""

from pathlib import Path
from tomllib import loads
from typing import TYPE_CHECKING

from . import cli

if TYPE_CHECKING:
    from .store import Store
    from .sync import Progress

KINDS: dict[str, set[str]] = {
    # anything rg can read: source, config, logs, subtitles, structured data
    "text": {"bash", "c", "cfg", "cjs", "conf", "cpp", "css", "csv", "diff", "env", "go", "h", "hpp", "htm", "html", "ini", "java", "jenkinsfile", "js", "json", "jsonl", "jsx", "kt", "log", "lua", "makefile", "markdown", "md", "mjs", "ndjson", "patch", "php", "pl", "properties", "py", "r", "rb", "rs", "rst", "scss", "sh", "sql", "srt", "svg", "swift", "tex", "text", "toml", "ts", "tsv", "tsx", "txt", "vtt", "xml", "yaml", "yml", "zsh"},
    "image": {"avif", "bmp", "gif", "heic", "ico", "jpeg", "jpg", "png", "tiff", "webp"},
    "video": {"avi", "mkv", "mov", "mp4", "webm"},
    "audio": {"aac", "m4a", "mp3", "opus", "wav"},
    "doc": {"doc", "docx", "key", "numbers", "pages", "pdf", "ppt", "pptx", "xls", "xlsx"},
    "archive": {"7z", "gz", "rar", "tar", "tgz", "zip", "zst"},
}

DEFAULT_CONFIG = f"""# lark-fs settings. Delete a line to go back to its default.

[attachments]
# Mirror the file itself, not just its key, for these kinds. Text is on by default because
# it is what makes `rg` reach inside attachments; images are not, because messages embed
# thousands of them and each one costs a request against the tenant's monthly quota.
# available: {", ".join(KINDS)}
kinds = ["text"]

# Checked after downloading -- Lark does not report a size in advance. Anything larger is
# deleted and remembered, so it is not fetched again.
max_mb = 10
"""


class Policy:
    def __init__(self, kinds: set[str], max_bytes: int):
        self.kinds, self.max_bytes = kinds, max_bytes

    def wants(self, key: str, name: str) -> bool:
        return self.kind(key, name) in self.kinds

    @staticmethod
    def kind(key: str, name: str) -> str:
        if not name:  # images embedded in a rich-text message arrive unnamed
            return "image" if key.startswith("img_") else ""
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else name.lower()
        return next((k for k, exts in KINDS.items() if ext in exts), "")


def policy_for(store: Store) -> Policy:
    """Read the store's settings, writing the annotated defaults the first time."""
    path = store.root / ".lark-fs/config.toml"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(DEFAULT_CONFIG)
    section = loads(path.read_text()).get("attachments", {})
    return Policy(set(section.get("kinds", ["text"])), int(section.get("max_mb", 10) * 1024 * 1024))


def _dir(store: Store, row: dict) -> Path:
    """One directory per key -- it deduplicates a file forwarded into several chats, and
    keeps the original filename intact next to it for `fd` to find."""
    return store.root / f"chats/{row['chat_id']}/files/{row['key']}"


def _settle(dest: Path, data: dict, cap: int):
    """Discard an attachment that turned out to be too big, and remember that it was."""
    saved = Path(data.get("saved_path") or "")
    if data.get("size_bytes", 0) > cap and saved.is_file():
        saved.unlink()
        (dest / ".oversize").write_text("")  # a marker, or every run re-downloads it to find out


def _pending(store: Store, policy: Policy, rows: list[dict]) -> list[dict]:
    """Rows still worth a request: admitted by the policy, and not already settled on disk.

    A directory with anything in it counts as settled -- including just the `.oversize`
    marker, which pathlib's `glob("*")` does match even though a shell's would not.
    """
    return [r for r in rows if r.get("key") and policy.wants(r["key"], r.get("name") or "") and not any(_dir(store, r).glob("*"))]


async def sync_attachments(store: Store, p: Progress):
    """Download every attachment the policy admits, from the media index already on disk."""
    p.set("files", state="running")
    policy = policy_for(store)
    rows = [r for f in (store.root / "chats").glob("*/media.yaml") for r in store.read_yaml_rows(f"chats/{f.parent.name}/media.yaml")]
    todo = _pending(store, policy, rows)
    p.set("files", total=len(todo), done=0, note=f"{len(rows)} attachments indexed")

    async def one(row: dict):
        key, name = row["key"], row.get("name") or ""
        dest = _dir(store, row)
        try:
            data = await cli.run(
                "im", "+messages-resources-download",
                "--message-id", row["message_id"], "--file-key", key,
                "--type", "image" if key.startswith("img_") else "file",
                # relative only, resolved against cwd; the CLI creates the directories itself
                "--output", str(dest.relative_to(store.root) / (name or key)),
                cwd=str(store.root), subject=f"fetch {cli.oneline(name or key, 40)}",
            )
        except cli.LarkError:
            return  # transient: leave it due, the next run retries it
        finally:
            p.bump("files", last=cli.oneline(name or key, 48))
        _settle(dest, data or {}, policy.max_bytes)

    await cli.spread(one, todo)
    p.set("files", state="done", note=f"{len(todo)} fetched, {len(rows)} indexed")
