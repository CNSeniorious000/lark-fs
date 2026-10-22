"""The credential material lark-cli already holds, read directly.

`lark-cli auth login` is still what authorises lark-fs -- a browser flow, a refresh
daemon and DPoP-aware storage are its job, not this project's. All the request path
needs is the token material it leaves behind: the app secret (plaintext in its config)
and the user access token (AES-GCM in its local keychain, master key beside it). The
file format is lark-cli's own -- accepted coupling; `LARKSUITE_CLI_DATA_DIR` and the
macOS/Windows keychain are out of scope, and a store that cannot be read sends the
caller back to the subprocess transport instead of failing.

Refreshes write the rotated token back through the same lock lark-cli takes
(`locks/refresh_<app>_<user>.lock`), because the OAuth refresh token rotates: two
refreshers keeping their own copies would leave one of them holding a token the server
has already invalidated.
"""

from contextlib import suppress
from dataclasses import dataclass, replace
from json import dumps, loads
from os import environ
from pathlib import Path
from re import compile as regex
from time import time

DATA_DIR = environ.get("LARKSUITE_CLI_DATA_DIR") or ""
CONFIG = Path.home() / ".lark-cli" / "config.json"
_SAFENAME = regex(r"[^a-zA-Z0-9._-]")
_REFRESH_AHEAD_MS = 5 * 60 * 1000  # lark-cli's own margin: hand out a token with room to spare


def _storage_dir() -> Path:
    return Path(DATA_DIR) / "lark-cli" if DATA_DIR else Path.home() / ".local" / "share" / "lark-cli"


def _account_file(app_id: str, user_open_id: str) -> Path:
    # lark-cli's account key is "<appId>:<userOpenId>", sanitised into a filename
    return _storage_dir() / (_SAFENAME.sub("_", f"{app_id}:{user_open_id}") + ".enc")


@dataclass(frozen=True)
class Credentials:
    app_id: str
    brand: str
    app_secret: str
    user_open_id: str
    access_token: str
    refresh_token: str
    expires_at: int  # unix ms
    refresh_expires_at: int  # unix ms
    token_type: str = "Bearer"

    @property
    def user_token_usable(self) -> bool:
        return self.token_type.lower() == "bearer" and self.expires_at - _REFRESH_AHEAD_MS > time() * 1000


def _decrypt(path: Path) -> str | None:
    """One AES-GCM record, or None when this platform's store is not the file one."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        return None
    try:
        key = (_storage_dir() / "master.key").read_bytes()
        blob = path.read_bytes()
    except OSError:
        return None
    if len(key) != 32 or len(blob) < 28:
        return None
    with suppress(Exception):  # a corrupt record reads as "not available", never as a crash
        return AESGCM(key).decrypt(blob[:12], blob[12:], None).decode()
    return None


def _encrypt(text: str) -> bytes | None:
    try:
        from os import urandom

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        return None
    key = (_storage_dir() / "master.key").read_bytes()
    iv = urandom(12)
    return iv + AESGCM(key).encrypt(iv, text.encode(), None)


def _lock(held) -> None:
    """Advisory: two refreshers sharing one record must not interleave read and write."""
    with suppress(Exception):
        import fcntl

        fcntl.flock(held, fcntl.LOCK_EX)


def load() -> Credentials | None:
    """Everything needed to speak to the API, or None when lark-cli's store is unreadable.

    The `--as user` half of every call is what the mirror runs on; a token type other
    than Bearer means DPoP took over and proving possession is lark-cli's game again.
    """
    try:
        config = loads(CONFIG.read_text())
    except OSError, ValueError:
        return None
    app = next((a for a in config.get("apps") or [] if isinstance(a, dict)), None)
    user = next((u for u in (app or {}).get("users") or [] if isinstance(u, dict) and u.get("userOpenId")), None)
    if not app or not user:
        return None
    app_id, user_open_id = app["appId"], user["userOpenId"]
    raw_secret = app.get("appSecret")
    secret = raw_secret if isinstance(raw_secret, str) else ""
    raw = _decrypt(_account_file(app_id, user_open_id))
    if raw is None:
        return None
    try:
        stored = loads(raw)
    except ValueError:
        return None
    return Credentials(
        app_id=app_id,
        brand=str(app.get("brand") or "feishu"),
        app_secret=secret,
        user_open_id=user_open_id,
        access_token=stored.get("accessToken") or "",
        refresh_token=stored.get("refreshToken") or "",
        expires_at=int(stored.get("expiresAt") or 0),
        refresh_expires_at=int(stored.get("refreshExpiresAt") or 0),
        token_type=stored.get("tokenType") or "Bearer",
    )


def save(creds: Credentials, payload: dict) -> Credentials:
    """Write a refreshed token back into lark-cli's own record, so both stay coherent.

    `payload` is the token endpoint's answer; the rest of the record -- scope, open id,
    the DPoP fields that do not apply to a Bearer token -- carries over untouched.
    """
    raw = _decrypt(_account_file(creds.app_id, creds.user_open_id))
    if raw is None:
        return creds
    try:
        stored = loads(raw)
    except ValueError:
        return creds
    ms = lambda seconds: int(time() * 1000 + int(seconds) * 1000)
    stored = {
        **stored,
        "accessToken": payload.get("access_token") or stored.get("accessToken"),
        "refreshToken": payload.get("refresh_token") or stored.get("refreshToken"),
        **({"expiresAt": ms(payload["expires_in"])} if payload.get("expires_in") else {}),
        **({"refreshExpiresAt": ms(payload["refresh_token_expires_in"])} if payload.get("refresh_token_expires_in") else {}),
    }
    blob = _encrypt(dumps(stored, ensure_ascii=False))
    if blob is None:
        return creds
    lock = Path.home() / ".lark-cli" / "locks" / f"refresh_{creds.app_id}_{creds.user_open_id}.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("w") as held:  # the same file lark-cli locks around its own refresh
        _lock(held)
        _account_file(creds.app_id, creds.user_open_id).write_bytes(blob)
    return replace(creds, access_token=stored["accessToken"], refresh_token=stored["refreshToken"], expires_at=stored["expiresAt"], refresh_expires_at=stored["refreshExpiresAt"])


def refresh_body(creds: Credentials) -> str:
    return dumps({"grant_type": "refresh_token", "refresh_token": creds.refresh_token, "client_id": creds.app_id, "client_secret": creds.app_secret})


def parse_token_reply(payload: dict) -> dict:
    """Accept both envelope shapes the token endpoints have used."""
    body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    return {k: v for k, v in (body or {}).items() if k in ("access_token", "refresh_token", "expires_in", "refresh_token_expires_in", "token_type")}
