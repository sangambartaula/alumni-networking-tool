"""Compatibility facade for sqlite fallback APIs.

This module keeps historical imports stable while implementation is split.
"""

try:
    from . import sqlite_fallback_core as _core
    from .sqlite_fallback_api import *
except ImportError:
    import sqlite_fallback_core as _core
    from sqlite_fallback_api import *


def __getattr__(name):
    return getattr(_core, name)


def __dir__():
    return sorted(set(globals().keys()) | set(dir(_core)))


if __name__ == "__main__":
    import os
    import runpy

    runpy.run_path(
        os.path.join(os.path.dirname(__file__), "sqlite_fallback_core.py"),
        run_name="__main__",
    )
