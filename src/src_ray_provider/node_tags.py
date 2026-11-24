from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Mapping

from ray.autoscaler.tags import TAG_RAY_LAUNCH_CONFIG

from .utils import locked_cache_file, sanitize_name_component


logger = logging.getLogger(__name__)

_WORKSPACE_TAGS_DESCRIPTION_PREFIX = "src-ray-provider-tags:v1:"
_IMMUTABLE_LAUNCH_TAGS = frozenset({TAG_RAY_LAUNCH_CONFIG})
_REUSE_CONFIGURATION_KEY = "src-reuse-configuration"


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_json_object(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path_str = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".json")
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        tmp_path.replace(path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


class _NodeTagCache:
    """Persist Ray's mutable, per-node tags to a local JSON file.

    SRC workspaces have no tag update operation, so mutable tags Ray writes
    after node creation are kept in
    ``~/.cache/src_ray_provider/node_tags/<cluster_name>.json``, keyed by SRC
    workspace id. A sibling ``.lock`` file provides cross-process safety for
    concurrent readers and writers on that machine.
    """

    def __init__(self, cluster_name: str, cache_dir: Path) -> None:
        self._path = cache_dir / f"{sanitize_name_component(cluster_name)}.json"
        self._lock_path = self._path.with_suffix(self._path.suffix + ".lock")
        self._process_lock = threading.RLock()

    def get(self, node_id: str) -> dict[str, str]:
        with self._process_lock, locked_cache_file(self._lock_path):
            cache = _read_json_object(self._path)
        tags = cache.get(node_id)
        return dict(tags) if isinstance(tags, dict) else {}

    def update(self, node_id: str, tags: Mapping[str, str]) -> None:
        if not tags:
            return
        with self._process_lock, locked_cache_file(self._lock_path):
            cache = _read_json_object(self._path)
            node_tags = cache.get(node_id)
            node_tags = dict(node_tags) if isinstance(node_tags, dict) else {}
            node_tags.update(tags)
            cache[node_id] = node_tags
            _write_json_object(self._path, cache)

    def replace(self, node_id: str, tags: Mapping[str, str]) -> None:
        with self._process_lock, locked_cache_file(self._lock_path):
            cache = _read_json_object(self._path)
            if tags:
                cache[node_id] = dict(tags)
            else:
                cache.pop(node_id, None)
            _write_json_object(self._path, cache)

    def discard(self, node_ids: list[str]) -> None:
        if not node_ids:
            return
        with self._process_lock, locked_cache_file(self._lock_path):
            cache = _read_json_object(self._path)
            changed = False
            for node_id in node_ids:
                if cache.pop(node_id, None) is not None:
                    changed = True
            if changed:
                _write_json_object(self._path, cache)


def _workspace_tags_description(
    tags: Mapping[str, str], reuse_configuration: str | None = None
) -> str:
    launch_tags = {
        key: value
        for key, value in tags.items()
        if key in _IMMUTABLE_LAUNCH_TAGS and isinstance(value, str)
    }
    if reuse_configuration is not None:
        launch_tags[_REUSE_CONFIGURATION_KEY] = reuse_configuration
    if not launch_tags:
        return ""
    return _WORKSPACE_TAGS_DESCRIPTION_PREFIX + json.dumps(launch_tags, separators=(",", ":"))


def _workspace_metadata_from_description(description: Any) -> dict[str, Any]:
    if not isinstance(description, str) or not description.startswith(_WORKSPACE_TAGS_DESCRIPTION_PREFIX):
        return {}
    encoded_tags = description[len(_WORKSPACE_TAGS_DESCRIPTION_PREFIX):]
    try:
        tags = json.loads(encoded_tags)
    except json.JSONDecodeError:
        logger.warning("Ignoring malformed Ray tags in workspace description")
        return {}
    if not isinstance(tags, dict):
        logger.warning("Ignoring malformed Ray tags in workspace description")
        return {}
    return tags


def _launch_tags_from_workspace_description(description: Any) -> dict[str, str]:
    return {
        key: value
        for key, value in _workspace_metadata_from_description(description).items()
        if key in _IMMUTABLE_LAUNCH_TAGS and isinstance(value, str)
    }
