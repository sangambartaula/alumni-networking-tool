"""Compatibility facade for database APIs.

This module remains import-compatible (`import database`, `from database import ...`)
while implementation code lives in smaller modules.
"""

try:
    from . import database_core as _core
    from .database_api import *
except ImportError:
    import database_core as _core
    from database_api import *


def __getattr__(name):
    return getattr(_core, name)


def __dir__():
    return sorted(set(globals().keys()) | set(dir(_core)))


if __name__ == "__main__":
    import os
    import runpy

    runpy.run_path(
        os.path.join(os.path.dirname(__file__), "database_core.py"),
        run_name="__main__",
    )
