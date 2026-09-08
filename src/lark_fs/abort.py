"""The cooperative-stop contract, kept out of every hot-reloaded module on purpose.

A reload rebinds a module's names to freshly executed objects, so a class defined in a
reloadable module is a *different* class after each reload. Two things break when this
one lives there:

- `except cli.SyncAbortedError` reads the current class while an exception already in
  flight carries the one from before the reload. Same name, different object, so the
  handler does not match and a routine reload prints a traceback (it did: a reload landing
  while `profiles` awaited `rosters` escaped hmr.py's handler entirely).
- `Aborted.flag` and `.reason` are class attributes, so a reload resets them. Losing the
  flag only costs a sweep, but losing the reason turns the one unsurvivable stop -- the
  tenant's monthly quota, which no rerun can clear until the 1st -- back into "interrupted;
  rerun to resume".

hmr.py excludes this file from the reloader, which pins both. Nothing here should ever
need editing mid-run: it is two names and a flag, and that is the point.
"""


class Aborted:
    """Cooperative stop. Every request is a checkpoint, so a sync interrupts promptly
    no matter which collection is running."""

    flag = False
    reason = ""  # why, when it was not a keystroke -- a stop the user cannot act on the same way

    @classmethod
    def check(cls):
        if cls.flag:
            raise SyncAbortedError


class SyncAbortedError(Exception):
    """Raised at a request boundary once a stop has been requested."""
