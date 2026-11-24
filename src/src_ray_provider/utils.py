from __future__ import annotations

import contextlib
import copy
import re
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover - fcntl is POSIX-only; Ray targets POSIX hosts
    fcntl = None


_NAME_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_\-]+")


def deep_merge(base: dict, override: dict) -> dict:
    """Return base overlaid with override."""
    out = copy.deepcopy(base)
    for key, val in override.items():
        if isinstance(val, list):
            existing = out.get(key, None)
            if existing is not None and not isinstance(existing, list):
                raise TypeError(f"'{key}' needs a list on both sides (got {type(existing).__name__} and {type(val).__name__})")
            out[key] = (existing or []) + copy.deepcopy(val)
        elif isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], val)
        else:
            out[key] = copy.deepcopy(val)
    return out


def sanitize_name_component(value: str) -> str:
    """Sanitize a string so it is safe to embed in an SRC workspace ``name``.

    SRC workspace names must match ``^[a-zA-Z0-9_\\-\\s]+$`` and are capped
    at 100 characters. This collapses any run of disallowed characters to a
    single dash and strips leading/trailing dashes.
    """
    sanitized = _NAME_SANITIZE_RE.sub("-", value.strip()).strip("-")
    if not sanitized:
        raise ValueError(f"cannot derive a valid SRC workspace name component from {value!r}")
    return sanitized


@contextlib.contextmanager
def locked_cache_file(lock_path: Path):
    """Hold an exclusive, cross-process advisory lock while touching a cache file.

    Guards against concurrent read-modify-write races between, e.g., a
    ``ray up`` invocation and the autoscaler monitor running against the
    same cache file. A no-op on platforms without ``fcntl`` (non-POSIX);
    Ray itself targets POSIX hosts for both the CLI and head node.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if fcntl is None:  # pragma: no cover - fcntl is POSIX-only
        yield
        return
    with open(lock_path, "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
