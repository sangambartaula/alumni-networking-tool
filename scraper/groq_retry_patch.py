"""
Configure Groq SDK HTTP retry backoff (429 / transient errors).

The official client retries /openai/v1/chat/completions with short exponential delays
by default. Import this module (or call apply_groq_retry_delay()) before constructing
any groq.Groq() client — including code paths that instantiate Groq outside groq_client.py.

Env:
  GROQ_RETRY_DELAY_SECONDS — seconds between retry attempts (default 5). Set 0 for SDK defaults.
"""

from __future__ import annotations

import os

_applied = False


def apply_groq_retry_delay() -> None:
    global _applied
    if _applied:
        return
    try:
        import groq._base_client as _bc
    except ImportError:
        return
    sec = float(os.getenv("GROQ_RETRY_DELAY_SECONDS", "5"))
    if sec > 0:
        _bc.INITIAL_RETRY_DELAY = sec
        _bc.MAX_RETRY_DELAY = sec
    _applied = True


apply_groq_retry_delay()
