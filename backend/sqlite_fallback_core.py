"""Facade for split sqlite fallback core implementation."""

try:
    from .sf_core_shared import *
    from .sf_core_manager import *
    from .sf_core_wrappers import *
    from .sf_core_runtime import *
except ImportError:
    from sf_core_shared import *
    from sf_core_manager import *
    from sf_core_wrappers import *
    from sf_core_runtime import *
