from .cli import LarkError, run
from .store import Store

__all__ = ["LarkError", "Store", "main", "run"]


def main():
    from .main import main as _main

    _main()
