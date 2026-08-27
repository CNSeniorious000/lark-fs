"""Fetching the attachments themselves, so a text one is greppable like any other file.

Lark never reports an attachment's size until it has been downloaded -- the message body
carries only a key and a filename -- so the size limit is enforced after the fact and the
kind whitelist is what actually bounds the request count.
"""

from collections.abc import Iterable
from pathlib import Path
from tomllib import loads
from typing import TYPE_CHECKING

from . import cli

if TYPE_CHECKING:
    from .store import Store
    from .sync import Progress

# anything rg can read: source, config, logs, subtitles, structured data
_TEXT = """bash c cfg cjs conf cpp css csv diff env go h hpp htm html ini java jenkinsfile js json jsonl jsx kt log lua
makefile markdown md mjs ndjson patch php pl properties py r rb rs rst scss sh sql srt svg swift tex text toml ts tsv
tsx txt vtt xml yaml yml zsh"""

OVERSIZE = ".oversize"  # marks a key that was fetched, measured, and discarded

KINDS: dict[str, set[str]] = {
    "text": set(_TEXT.split()),
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

# Fetched whatever their kind, for extensions the lists above miss. A tenant's own
# conventions live here: `.jsonl.gz` dumps, `.prompt` files, an in-house log suffix.
extensions = []
"""


class Policy:
    def __init__(self, kinds: set[str], max_bytes: int, extensions: Iterable[str] = ()):
        self.kinds, self.max_bytes = kinds, max_bytes
        self.extra = {e.lower().lstrip(".") for e in extensions}

    def wants(self, key: str, name: str) -> bool:
        return (bool(name) and _ext(name) in self.extra) or self.kind(key, name) in self.kinds

    @staticmethod
    def kind(key: str, name: str) -> str:
        if not name:  # images embedded in a rich-text message arrive unnamed
            return "image" if key.startswith("img_") else ""
        return next((k for k, exts in KINDS.items() if _ext(name) in exts), "")


def _ext(name: str) -> str:
    return (name.rsplit(".", 1)[-1] if "." in name else name).lower()


def policy_for(store: Store) -> Policy:
    """Read the store's settings, writing the annotated defaults the first time."""
    path = store.root / ".lark-fs/config.toml"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(DEFAULT_CONFIG)
    section = loads(path.read_text()).get("attachments", {})
    return Policy(set(section.get("kinds", ["text"])), int(section.get("max_mb", 10) * 1024 * 1024), section.get("extensions", ()))


def _dir(store: Store, row: dict) -> Path:
    """One directory per key -- it deduplicates a file forwarded into several chats, and
    keeps the original filename intact next to it for `fd` to find."""
    return store.root / f"chats/{row['chat_id']}/files/{row['key']}"


def _suffix_for(head: bytes) -> str:
    """Lark's rich-text images arrive with no filename and no content type, and the key
    says `img_` without saying which kind, so the bytes are the only thing that knows."""
    if head.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if head.startswith(b"GIF8"):
        return ".gif"
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return ".webp"
    return ""


def _settle(dest: Path, data: dict, cap: int) -> bool:
    """Discard an attachment that turned out to be too big; True when it was kept."""
    saved = Path(data.get("saved_path") or "")
    if (size := data.get("size_bytes", 0)) > cap and saved.is_file():
        saved.unlink()
        # the marker carries the size that disqualified it, so raising max_mb brings it back;
        # a bare marker would make the first cap that rejected a file the permanent one
        (dest / OVERSIZE).write_text(str(size))
        return False
    if saved.is_file() and not saved.suffix and (suffix := _suffix_for(saved.read_bytes()[:12])):
        # named by the key alone it is neither openable nor reachable by `fd -e jpg`
        saved.rename(saved.with_name(saved.name + suffix))
    return True


def _settled(d: Path, cap: int) -> bool:
    """Is this key done with, for the current size cap?

    Anything in the directory counts -- including just the marker, which pathlib's
    `glob("*")` does match even though a shell's would not. A marker written before this
    change has no size in it, and re-reading such a file once is what fills it in.
    """
    if not any(d.glob("*")):
        return False
    if (mark := d / OVERSIZE).is_file():
        return int(mark.read_text() or 0) > cap
    return True


def _pending(store: Store, policy: Policy, rows: list[dict]) -> list[dict]:
    """Rows still worth a request: admitted by the policy, and not already settled on disk."""
    return [r for r in rows if r.get("key") and policy.wants(r["key"], r.get("name") or "") and not _settled(_dir(store, r), policy.max_bytes)]


async def sync_attachments(store: Store, p: Progress):
    """Download every attachment the policy admits, from the media index already on disk."""
    p.set("files", state="running")
    policy = policy_for(store)
    rows = store.glob_rows("chats/*/media.yaml")
    todo = _pending(store, policy, rows)
    p.set("files", total=len(todo), done=0, note=f"{len(rows)} attachments indexed")
    kept = 0

    async def one(row: dict):
        nonlocal kept
        key, name = row["key"], row.get("name") or ""
        dest = _dir(store, row)
        out = dest.relative_to(store.root) / (name or key)  # relative only; the CLI creates the directories itself
        argv = ["im", "+messages-resources-download", "--message-id", row["message_id"], "--file-key", key, "--type", "image" if key.startswith("img_") else "file", "--output", str(out)]
        try:
            data = await cli.run(*argv, cwd=str(store.root), subject=f"fetch {cli.oneline(name or key, 40)}")
        except cli.LarkError:
            return  # transient: leave it due, the next run retries it
        finally:
            p.bump("files", last=cli.oneline(name or key, 48))
        kept += _settle(dest, data or {}, policy.max_bytes)

    await cli.spread(one, todo)
    # attempted and kept differ whenever the size cap bites, and only the second one is on disk
    p.set("files", state="done", note=f"{kept} kept of {len(todo)} tried, {len(rows)} indexed")
