"""Centralized import for the optional ormar_rust_utils Rust accelerator."""

try:  # pragma: no cover
    import ormar_rust_utils

    HAS_RUST = True
except ImportError:  # pragma: no cover
    HAS_RUST = False
    ormar_rust_utils = None  # type: ignore[assignment]
