"""Python bindings for the Rust implementation."""

# Re-export all symbols from the compiled extension. Ruff would normally flag
# this with ``F401`` (unused imports) and ``F403`` (``import *``). We silence
# those warnings so users can simply ``import datadrill_rs``.
from .datadrill_rs import *  # noqa: F401,F403
