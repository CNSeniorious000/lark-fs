"""Dependency root for the HMR reloader (see watch.py).

Importing the package here is the whole point: the reloader tracks whatever its entry
touches, so an empty file would give it nothing to watch and reloads would never fire
(it says so, as "has no dependencies and will never be auto-triggered").

Kept separate from watch.py because the reloader re-executes its entry on every reload,
and re-executing watch.py would recurse into itself.
"""

from lark_fs import cli, daemon, main, reindex, store, sync, tui, yaml  # noqa: F401
