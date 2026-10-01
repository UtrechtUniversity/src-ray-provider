"""SURF ResearchCloud node provider for Ray.

This slice discovers workspaces as Ray nodes and derives their static,
creation-time tags (node kind, user node type, cluster name) entirely from
queryable workspace fields — no tag data is persisted in the workspace
itself. Node creation and SSH address lookup are supported; mutable tag
storage is not needed, and termination delegates to the workspace delete API.

Static tag derivation works as follows, trading a small amount of config
duplication for avoiding any persisted/free-text tag storage:

- ``TAG_RAY_USER_NODE_TYPE`` is recovered by reverse-mapping a workspace's
  ``(catalog item name, size flavour name)`` pair back to the Ray node type
  that was configured to use that pair (``provider.config.node_types``).
  This requires each node type to resolve to a distinct pair; this is
  validated at startup. SRC catalog items are role-specific (distinct head
  vs. worker offerings), so the default catalog item name also depends on
  whether a node type is the configured ``head_node_type`` (see
  ``DEFAULT_HEAD_CATALOG_ITEM_NAME`` / ``DEFAULT_WORKER_CATALOG_ITEM_NAME``);
  it can still be overridden per node type via
  ``available_node_types.<type>.node_config.catalog_item_name``.
- ``TAG_RAY_NODE_KIND`` is a pure function of the derived node type versus
  ``provider.config.head_node_type`` — a value Ray does not pass to
  ``NodeProvider.__init__`` (only the ``provider:`` section and
  ``cluster_name`` are passed). Rather than requiring cluster.yaml authors
  to duplicate it manually, ``bootstrap_config`` (a hook Ray calls once
  with the *full* cluster config, before constructing the provider; see
  ``ray.autoscaler.node_provider.NodeProvider.bootstrap_config`` and
  ``ray.autoscaler._private.commands._bootstrap_config``) copies
  cluster.yaml's top-level ``head_node_type`` into ``provider.head_node_type``
  automatically. Likewise, ``provider.node_types`` is auto-derived from
  each node type's ``available_node_types.<type>.node_config`` — the same
  place Ray's own providers read provider-specific per-node-type settings
  from (e.g. the AWS provider's ``create_node(node_config, ...)``) — so no
  parallel config block needs to be hand-written either. Both remain
  overridable by setting them explicitly under ``provider:``.
- ``TAG_RAY_CLUSTER_NAME`` is never parsed back out of anything: since a
  ``NodeProvider`` only ever manages nodes within its own ``cluster_name``
  namespace, it is simply ``self.cluster_name`` for any workspace that
  belongs to this cluster. Cluster membership is determined by a naming
  convention applied when a workspace is created: its ``name`` is prefixed
  with ``ray-{cluster_name}-`` (sanitized to fit SRC's naming rules).

Mutable Ray tags (notably ``TAG_RAY_LAUNCH_CONFIG``, written once at node
creation and read back by ``ray up``/the autoscaler on every subsequent
invocation to decide whether a node is out-of-date) have nowhere to live in
SRC itself: the workspace API has no generic tag/label/metadata store. Since
losing ``TAG_RAY_LAUNCH_CONFIG`` makes every node look permanently
out-of-date -- causing ``ray up`` to terminate and recreate an otherwise
healthy head node on every single run -- these tags are cached in a local
JSON file under ``~/.cache/src_ray_provider/node_tags/<cluster_name>.json``
(see ``_NodeTagCache``). This is necessarily local to the machine/account
running a given Ray command: a laptop running ``ray up`` and the autoscaler
monitor running on the head node each keep their own cache file, so this
only fixes repeat invocations from the *same* machine (which is the normal
case for both ``ray up`` and the head-resident autoscaler monitor).

SSH access for both the head and worker nodes relies on ``auth.ssh_public_key``
/``auth.ssh_private_key`` in cluster.yaml, which ``bootstrap_config`` requires
to be set together (or left unset together) -- see its docstring for why only
setting one silently breaks head-to-worker SSH. When both are left unset,
nothing in Ray generates credentials for the ``external`` provider type on
its own (unlike its AWS/vSphere providers), so ``bootstrap_config`` generates
and caches its own ed25519 keypair per cluster name instead (see
``_ensure_generated_keypair``), under
``~/.cache/src_ray_provider/ssh_keys/<cluster_name>/``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import platform
import re
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

try:
    import fcntl
except ImportError:  # pragma: no cover - fcntl is POSIX-only; Ray targets POSIX hosts
    fcntl = None

from ray import _version as ray_version
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)

from researchcloud.client import ResearchCloudClient
from researchcloud.config import DEFAULT_CLOUD_NAME
from researchcloud.errors import ApiError
from researchcloud.services import is_workspace_terminal_status
from researchcloud.utils.flavours import match_size_flavour, validate_size_flavour_selection


DEFAULT_HEAD_CATALOG_ITEM_NAME = "Ray Head Node"
DEFAULT_WORKER_CATALOG_ITEM_NAME = "Ray Worker"
DEFAULT_OS_FLAVOUR_NAME = "Ubuntu 24.04"
DEFAULT_WORKSPACE_CREATION_TIMEOUT = 2400
WORKSPACE_CREATION_POLL_INTERVAL = 5

HEAD_SETUP_COMMANDS = [] # default head setup commands, currenly empty

HTTP_NOT_FOUND = 404

_NAME_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_\-]+")
_WORKSPACE_NAME_MAX_LENGTH = 100
logger = logging.getLogger(__name__)

NODE_TAG_CACHE_DIR = Path.home() / ".cache" / "src_ray_provider" / "node_tags"
SSH_KEY_CACHE_DIR = Path.home() / ".cache" / "src_ray_provider" / "ssh_keys"


@contextlib.contextmanager
def _locked_cache_file(lock_path: Path):
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

    SRC workspaces have no generic tag/label/metadata store, so tags Ray
    writes after node creation (notably ``TAG_RAY_LAUNCH_CONFIG``, used to
    detect out-of-date nodes) cannot be saved on the workspace itself. This
    keeps them in ``~/.cache/src_ray_provider/node_tags/<cluster_name>.json``
    instead, keyed by SRC workspace id, so they survive across separate
    ``ray`` command invocations on the same machine/account. A sibling
    ``.lock`` file provides cross-process safety for concurrent readers and
    writers (e.g. ``ray up`` and the autoscaler monitor) on that machine.
    """

    def __init__(self, cluster_name: str, cache_dir: Path) -> None:
        self._path = cache_dir / f"{sanitize_name_component(cluster_name)}.json"
        self._lock_path = self._path.with_suffix(self._path.suffix + ".lock")
        self._process_lock = threading.RLock()

    def get(self, node_id: str) -> dict[str, str]:
        with self._process_lock, _locked_cache_file(self._lock_path):
            cache = _read_json_object(self._path)
        tags = cache.get(node_id)
        return dict(tags) if isinstance(tags, dict) else {}

    def update(self, node_id: str, tags: Mapping[str, str]) -> None:
        if not tags:
            return
        with self._process_lock, _locked_cache_file(self._lock_path):
            cache = _read_json_object(self._path)
            node_tags = cache.get(node_id)
            node_tags = dict(node_tags) if isinstance(node_tags, dict) else {}
            node_tags.update(tags)
            cache[node_id] = node_tags
            _write_json_object(self._path, cache)

    def discard(self, node_ids: list[str]) -> None:
        if not node_ids:
            return
        with self._process_lock, _locked_cache_file(self._lock_path):
            cache = _read_json_object(self._path)
            changed = False
            for node_id in node_ids:
                if cache.pop(node_id, None) is not None:
                    changed = True
            if changed:
                _write_json_object(self._path, cache)


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


class ResearchCloudNodeProvider(NodeProvider):
    """Discover SRC Compute workspaces as Ray nodes.

    Provider settings use ``co_name`` and ``wallet_name``; cloud, OS flavour,
    and network name hint have defaults or are optional. ``head_node_type``
    and ``node_types`` are required at construction time but are normally
    auto-populated by ``bootstrap_config`` rather than hand-written — see
    that method and the module docstring for how and why. Workspaces are
    configured to use the private network for node-to-node SSH.
    """

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Fill in ``provider.head_node_type``/``provider.node_types`` from
        cluster.yaml's top-level ``head_node_type``/``available_node_types``
        so cluster.yaml authors don't have to duplicate them under
        ``provider:`` by hand. Existing ``provider:`` values are left alone,
        so an explicit override still takes precedence.
        """
        provider_config = cluster_config.setdefault("provider", {})
        provider_config.setdefault("head_node_type", cluster_config.get("head_node_type"))

        head_setup_commands = cluster_config.setdefault("head_setup_commands", [])
        if not isinstance(head_setup_commands, list):
            raise ValueError("cluster config 'head_setup_commands' must be a list")
        for cmd in HEAD_SETUP_COMMANDS:
            head_setup_commands.append(cmd) if cmd not in head_setup_commands else None

        auth_config = cluster_config.setdefault("auth", {})
        if not isinstance(auth_config, dict):
            raise ValueError("cluster config 'auth' must be a mapping")

        public_key = auth_config.get("ssh_public_key")
        private_key = auth_config.get("ssh_private_key")
        has_public_key = isinstance(public_key, str) and bool(public_key.strip())
        has_private_key = isinstance(private_key, str) and bool(private_key.strip())
        if has_public_key != has_private_key:
            # Ray only copies auth.ssh_private_key onto the head node (as
            # ~/ray_bootstrap_key.pem, see ray.autoscaler._private.commands
            # ._set_up_config_for_head_node) when it is explicit, and this
            # provider can only turn a public key into the SRC catalog
            # item's ray_public_key parameter (below) if one is given.
            # Without both, the head's autoscaler monitor has no private key
            # to SSH into newly created workers with -- so workers get
            # created successfully but every SSH attempt into them fails
            # with "permission denied". Fail fast here instead of letting
            # that surface confusingly later during autoscaling.
            raise ValueError(
                "cluster config 'auth.ssh_public_key' and 'auth.ssh_private_key' must "
                "both be set, or both be left unset (in which case this provider "
                "generates and reuses its own keypair)"
            )

        if not has_public_key and not has_private_key:
            # Nothing in Ray generates SSH credentials for the "external"
            # provider type on its own (unlike e.g. the AWS or vSphere
            # providers' bootstrap_config, which create a keypair when
            # auth.ssh_private_key is absent). Without one, SRC workspaces
            # get created with no authorized key at all and nothing can SSH
            # into them. Generate (or reuse, across repeated `ray up` runs)
            # a keypair dedicated to this cluster name instead.
            generated_private_key, generated_public_key = ResearchCloudNodeProvider._ensure_generated_keypair(
                cluster_config.get("cluster_name")
            )
            auth_config["ssh_public_key"] = Path(generated_public_key).read_text(encoding="utf-8").strip()
            auth_config["ssh_private_key"] = Path(generated_private_key).read_text(encoding="utf-8").strip()
            public_key = auth_config["ssh_public_key"]

        if "ray_public_key" not in provider_config:
            public_key_path = Path(public_key).expanduser()
            provider_config["ray_public_key"] = (
                public_key_path.read_text(encoding="utf-8").strip()
                if public_key_path.is_file()
                else public_key.strip()
            )

        if "node_types" not in provider_config:
            derived_node_types: dict[str, dict[str, Any]] = {}
            for node_type, node_type_config in cluster_config.get("available_node_types", {}).items():
                node_config = node_type_config.get("node_config", {}) if isinstance(node_type_config, Mapping) else {}
                derived_node_type: dict[str, Any] = {
                    key: node_config[key]
                    for key in ("size_flavour_name", "num_cpu", "num_gpu", "gpu_type", "catalog_item_name")
                    if key in node_config
                }
                derived_node_types[node_type] = derived_node_type
            provider_config["node_types"] = derived_node_types

        return cluster_config

    @staticmethod
    def _ensure_generated_keypair(cluster_name: Any) -> tuple[Path, Path]:
        """Generate (or reuse) a local ed25519 keypair for clusters that
        don't configure their own ``auth.ssh_public_key``/``ssh_private_key``.

        Mirrors the pattern Ray's built-in cloud providers use when no key
        is configured (e.g. AWS's ``_configure_key_pair``, vSphere's
        ``configure_key_pair``): the "external" provider type has no such
        fallback built in, so without this, no explicit keys means no SSH
        access to either the head node or any worker nodes at all. The
        keypair is cached under ``SSH_KEY_CACHE_DIR``, keyed by cluster
        name, so repeated ``ray up`` runs reuse the same identity instead of
        generating a new, unrecognized key (and SRC workspace) every time.

        Returns the ``(private_key_path, public_key_path)`` pair.
        """
        if not isinstance(cluster_name, str) or not cluster_name.strip():
            raise ValueError(
                "cluster config 'cluster_name' must be a non-empty string to auto-generate SSH keys"
            )
        key_dir = SSH_KEY_CACHE_DIR / sanitize_name_component(cluster_name)
        private_key_path = key_dir / "id_ed25519"
        public_key_path = key_dir / "id_ed25519.pub"

        if not private_key_path.is_file() or not public_key_path.is_file():
            key_dir.mkdir(parents=True, exist_ok=True)
            private_key = ed25519.Ed25519PrivateKey.generate()
            private_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.OpenSSH,
                encryption_algorithm=serialization.NoEncryption(),
            )
            public_bytes = private_key.public_key().public_bytes(
                encoding=serialization.Encoding.OpenSSH,
                format=serialization.PublicFormat.OpenSSH,
            )
            fd = os.open(private_key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as fh:
                fh.write(private_bytes)
            public_key_path.write_bytes(public_bytes + b"\n")

        return private_key_path, public_key_path

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        super().__init__(provider_config, cluster_name)
        self.co_name = self._config_value(provider_config, "co_name")
        self.wallet_name = self._config_value(provider_config, "wallet_name")
        self.cloud_name = self._config_value(provider_config, "cloud_name", default=DEFAULT_CLOUD_NAME)
        self.os_flavour_name = self._config_value(
            provider_config, "os_flavour_name", default=DEFAULT_OS_FLAVOUR_NAME
        )
        self.network_name_hint = self._config_value(provider_config, "network_name_hint", optional=True)
        self.ray_public_key = self._config_value(provider_config, "ray_public_key", optional=True)
        self.workspace_creation_timeout = self._timeout_value(
            provider_config, "workspace_creation_timeout", DEFAULT_WORKSPACE_CREATION_TIMEOUT
        )

        self.head_node_type = self._config_value(provider_config, "head_node_type")
        self._node_type_configs, self._node_type_lookup = self._parse_node_types(provider_config)
        if self.head_node_type not in self._node_type_configs:
            raise ValueError(
                f"provider config 'head_node_type' {self.head_node_type!r} must be a key in 'node_types'"
            )
        self._cluster_prefix = f"ray-{sanitize_name_component(cluster_name)}-"

        self._workspaces: dict[str, dict[str, Any]] = {}
        self._workspaces_lock = threading.RLock()
        self._node_tag_cache = _NodeTagCache(cluster_name, NODE_TAG_CACHE_DIR)

    @staticmethod
    def _config_value(
        provider_config: Mapping[str, Any],
        key: str,
        default: str | None = None,
        *,
        optional: bool = False,
    ) -> str | None:
        """Read and validate a string provider-config value.

        With no ``default``, ``key`` is required unless ``optional=True``
        (in which case a missing value returns ``None``). Whenever a value
        is present (explicit or via ``default``), it must be a non-empty
        string.
        """
        value = provider_config.get(key, default)
        if value is None:
            if optional:
                return None
            raise ValueError(f"provider config must include a non-empty {key!r}")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"provider config {key!r} must be a non-empty string")
        return value.strip()

    @staticmethod
    def _timeout_value(provider_config: Mapping[str, Any], key: str, default: float) -> float:
        value = provider_config.get(key, default)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"provider config {key!r} must be a positive number of seconds")
        return float(value)

    def _workspace_creation_options(self, node_type: str) -> dict[str, Any]:
        node_type_config = self._node_type_configs[node_type]
        options = {
            "catalog_item_name": node_type_config["catalog_item_name"],
            "cloud_name": self.cloud_name,
            "os_flavour_name": self.os_flavour_name,
            "size_flavour_name": node_type_config["size_flavour_name"],
            "use_private_network": True,
            "network_name_hint": self.network_name_hint,
        }

        options["optional_parameters"] = {
            "ray_public_key": self.ray_public_key or "",
            "ray_do_setup": "false",
            "ray_version": ray_version.version,
            "ray_python_version": platform.python_version()
        }
        return options

    def create_node(
        self,
        node_config: Dict[str, Any],
        tags: Dict[str, str],
        count: int,
    ) -> Dict[str, Dict[str, Any]]:
        """Create up to ``count`` SRC workspaces for one Ray node type.

        The autoscaler monitor calls ``create_node_with_resources_and_labels``
        for worker scale-up, but ``ray up``'s head-node bootstrap
        (``ray.autoscaler._private.commands.get_or_create_head_node``) calls
        ``create_node`` directly, bypassing the resources/labels variant. SRC
        has no workspace-creation fields for either, so this just delegates
        with empty resources/labels.
        """
        return self.create_node_with_resources_and_labels(node_config, tags, count, {}, {})

    def create_node_with_resources_and_labels(
        self,
        node_config: Dict[str, Any],
        tags: Dict[str, str],
        count: int,
        resources: Dict[str, float],
        labels: Dict[str, str],
    ) -> Dict[str, Dict[str, Any]]:
        """Create up to ``count`` SRC workspaces for one Ray node type.

        SRC has no workspace-creation fields for Ray's scheduling resources
        or labels, so those hints are intentionally not persisted. Each
        workspace is submitted sequentially to avoid racing while resolving
        or creating the shared private network. API rejections are isolated
        to their workspace; other errors propagate.

        ``tags`` (e.g. ``TAG_RAY_LAUNCH_CONFIG``, ``TAG_RAY_NODE_STATUS``)
        are cached locally per created workspace id -- see ``_NodeTagCache``
        -- since SRC has nowhere to store them and losing them would make
        every node look permanently out-of-date to Ray.
        """
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError(f"node creation count must be a non-negative integer, got {count!r}")
        if count == 0:
            return {}

        node_type = tags.get(TAG_RAY_USER_NODE_TYPE)
        if not isinstance(node_type, str) or node_type not in self._node_type_configs:
            raise ValueError(f"node creation tags must include a configured {TAG_RAY_USER_NODE_TYPE!r}")
        # TAG_RAY_CLUSTER_NAME is intentionally not validated here: Ray never
        # includes it in the tags passed to create_node(_with_resources_and_labels)
        # for either the head node (ray.autoscaler._private.commands) or workers
        # (ray.autoscaler._private.node_launcher) — every built-in provider just
        # assumes it is always self.cluster_name instead of expecting callers to
        # supply it.
        expected_node_kind = self._kind_for_node_type(node_type)
        if tags.get(TAG_RAY_NODE_KIND) != expected_node_kind:
            raise ValueError(
                f"node creation tag {TAG_RAY_NODE_KIND!r} must be {expected_node_kind!r}, "
                f"got {tags.get(TAG_RAY_NODE_KIND)!r}"
            )

        del node_config, resources, labels
        created = asyncio.run(self._create_nodes(node_type, count))
        for workspace_id in created:
            self._node_tag_cache.update(workspace_id, tags)
        return created

    async def _create_nodes(self, node_type: str, count: int) -> Dict[str, Dict[str, Any]]:
        created: Dict[str, Dict[str, Any]] = {}
        async with ResearchCloudClient.from_env() as client:
            for _ in range(count):
                workspace_name = self._workspace_name_for(node_type)
                plan = await client.workspaces.build_create_payload_from_names(
                    co_name=self.co_name,
                    wallet_name=self.wallet_name,
                    workspace_name=workspace_name,
                    **self._workspace_creation_options(node_type),
                )
                try:
                    workspace = await client.workspaces.create(plan.payload)
                except ApiError as exc:
                    logger.error(
                        "Failed to create SRC workspace %r for Ray node type %r (HTTP %s): %s",
                        workspace_name,
                        node_type,
                        exc.status_code,
                        exc.body,
                    )
                    continue

                workspace_id = self._workspace_id(workspace)
                workspace = await self._wait_for_workspace_creation(client, workspace_id, workspace)
                created[workspace_id] = workspace
        return created

    async def _wait_for_workspace_creation(
        self,
        client: ResearchCloudClient,
        workspace_id: str,
        workspace: dict[str, Any],
    ) -> dict[str, Any]:
        """Wait for SRC to finish its ``creating`` phase before returning a node to Ray."""
        if self._workspace_status(workspace) != "creating":
            return workspace

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.workspace_creation_timeout
        while self._workspace_status(workspace) == "creating":
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(
                    f"SRC workspace {workspace_id!r} remained in 'creating' state for "
                    f"{self.workspace_creation_timeout:g} seconds"
                )
            await asyncio.sleep(min(WORKSPACE_CREATION_POLL_INTERVAL, remaining))
            workspace = await client.workspaces.get(workspace_id)
            self._workspace_id(workspace)
            self._workspace_status(workspace)
        return workspace

    def _default_catalog_item_name(self, node_type: str) -> str:
        return DEFAULT_HEAD_CATALOG_ITEM_NAME if node_type == self.head_node_type else DEFAULT_WORKER_CATALOG_ITEM_NAME

    def _node_type_sizing_spec(self, node_type: str, node_type_config: Mapping[str, Any]) -> dict[str, Any]:
        """Extract and validate a node type's sizing spec: an exact
        ``size_flavour_name``, or ``num_cpu``/``num_gpu`` (optionally with
        ``gpu_type``) to be resolved against the SRC catalog later. Exactly
        one sizing strategy must be given.
        """
        size_flavour_name = node_type_config.get("size_flavour_name")
        num_cpu = node_type_config.get("num_cpu")
        num_gpu = node_type_config.get("num_gpu")
        gpu_type = node_type_config.get("gpu_type")
        try:
            validate_size_flavour_selection(size_flavour_name, num_cpu, num_gpu)
        except ValueError as exc:
            raise ValueError(f"provider config node_types[{node_type!r}]: {exc}") from exc
        if size_flavour_name is not None and not (
            isinstance(size_flavour_name, str) and size_flavour_name.strip()
        ):
            raise ValueError(
                f"provider config node_types[{node_type!r}].size_flavour_name must be a non-empty string"
            )
        return {
            "size_flavour_name": size_flavour_name.strip() if size_flavour_name else None,
            "num_cpu": num_cpu,
            "num_gpu": num_gpu,
            "gpu_type": gpu_type,
        }

    async def _resolve_size_flavour_names(self, unresolved: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
        """Resolve ``num_cpu``/``num_gpu`` sizing specs to actual SRC size
        flavour names, by matching each node type's catalog item + offering
        against the catalog (see ``WorkspacesService.build_create_payload_from_names``,
        which does the equivalent resolution at workspace-creation time).
        """
        resolved: dict[str, str] = {}
        async with ResearchCloudClient.from_env() as client:
            co = await client.resolve_co(self.co_name)
            wallet = await client.resolve_wallet(self.wallet_name)
            products = wallet["budgets"][0]["products"]

            catalog_items: dict[str, dict[str, Any]] = {}
            for node_type, spec in unresolved.items():
                catalog_item_name = spec["catalog_item_name"]
                catalog_item = catalog_items.get(catalog_item_name)
                if catalog_item is None:
                    catalog_item = await client.resolve_catalog_item(catalog_item_name, co["id"], products)
                    catalog_items[catalog_item_name] = catalog_item

                offering, _, _ = await client.resolve_offering_and_flavours(
                    catalog_item, co["id"], products, self.cloud_name, self.os_flavour_name, None
                )
                size_flavour = match_size_flavour(
                    offering.get("flavours", []),
                    num_cpu=spec["num_cpu"],
                    num_gpu=spec["num_gpu"],
                    gpu_type=spec["gpu_type"],
                )
                resolved[node_type] = size_flavour["name"]
        return resolved

    def _parse_node_types(
        self, provider_config: Mapping[str, Any]
    ) -> tuple[dict[str, dict[str, str]], dict[tuple[str, str], str]]:
        """Build the node-type reverse-mapping from ``provider.config.node_types``.

        For each node type, resolves its catalog item (defaulting by role;
        see ``_default_catalog_item_name``) and size flavour name (resolving
        ``num_cpu``/``num_gpu`` sizing specs against the SRC catalog, if
        used), then indexes node types by that ``(catalog item, size
        flavour)`` pair — see the module docstring for why and how this
        reverse-mapping is used. Raises if two node types resolve to the
        same pair, since the mapping would then be ambiguous.
        """
        raw = provider_config.get("node_types")
        if not isinstance(raw, Mapping) or not raw:
            raise ValueError(
                "provider config must include a non-empty 'node_types' mapping that mirrors "
                "cluster.yaml's available_node_types, giving each node type's SRC sizing "
                "(size_flavour_name or num_cpu/num_gpu, optional catalog_item_name) so nodes "
                "can be matched back to their Ray node type without persisted tag storage"
            )

        specs: dict[str, dict[str, Any]] = {}
        for node_type, node_type_config in raw.items():
            if not isinstance(node_type_config, Mapping):
                raise ValueError(f"provider config node_types[{node_type!r}] must be a mapping")

            catalog_item_name = node_type_config.get(
                "catalog_item_name", self._default_catalog_item_name(node_type)
            )
            if not isinstance(catalog_item_name, str) or not catalog_item_name.strip():
                raise ValueError(
                    f"provider config node_types[{node_type!r}].catalog_item_name must be a non-empty string"
                )

            sizing = self._node_type_sizing_spec(node_type, node_type_config)
            specs[node_type] = {"catalog_item_name": catalog_item_name.strip(), **sizing}

        unresolved = {
            node_type: spec for node_type, spec in specs.items() if spec["size_flavour_name"] is None
        }
        if unresolved:
            resolved_names = asyncio.run(self._resolve_size_flavour_names(unresolved))
            for node_type, size_flavour_name in resolved_names.items():
                specs[node_type]["size_flavour_name"] = size_flavour_name

        configs: dict[str, dict[str, str]] = {}
        lookup: dict[tuple[str, str], str] = {}
        for node_type, spec in specs.items():
            catalog_item_name = spec["catalog_item_name"]
            size_flavour_name = spec["size_flavour_name"]
            key = (catalog_item_name, size_flavour_name)
            if key in lookup:
                raise ValueError(
                    f"node types {lookup[key]!r} and {node_type!r} both resolve to the same SRC "
                    f"catalog item + size flavour {key!r}; node types must be distinguishable to "
                    "derive the Ray user node type tag without persisted tag storage"
                )
            lookup[key] = node_type
            configs[node_type] = {"catalog_item_name": catalog_item_name, "size_flavour_name": size_flavour_name}
        return configs, lookup

    @staticmethod
    def _size_flavour_name_for_workspace(workspace: Mapping[str, Any]) -> str | None:
        """Return the catalog size flavour's ``name`` for a workspace.

        ``resource_meta.flavor_name`` is an infrastructure-level slug (e.g.
        ``"hpc-1core-8gb-20gb"``) that does not match the catalog flavour
        ``name`` (e.g. ``"1 Core - 8 GB RAM"``) node types are configured
        with. The catalog flavour actually applied to the workspace is
        listed in ``meta.flavours`` instead (the same objects
        ``resolve_offering_and_flavours``/``match_size_flavour`` match
        against at creation time), so the size flavour's ``name`` must be
        read from there.
        """
        flavours = workspace.get("meta", {}).get("flavours")
        if not isinstance(flavours, list):
            return None
        for flavour in flavours:
            if isinstance(flavour, Mapping) and flavour.get("category") == "size":
                name = flavour.get("name")
                if isinstance(name, str) and name:
                    return name
        return None

    def _node_type_for_workspace(self, workspace: Mapping[str, Any]) -> str | None:
        catalog_item_name = workspace.get("meta", {}).get("application_name")
        size_flavour_name = self._size_flavour_name_for_workspace(workspace)
        if not catalog_item_name or not size_flavour_name:
            return None
        return self._node_type_lookup.get((catalog_item_name, size_flavour_name))

    def _kind_for_node_type(self, node_type: str) -> str:
        return NODE_KIND_HEAD if node_type == self.head_node_type else NODE_KIND_WORKER

    def _node_tags_for_workspace(self, workspace: Mapping[str, Any]) -> dict[str, str]:
        """Derive this workspace's static Ray tags from queryable fields only.

        The cluster name tag is always ``self.cluster_name`` once a
        workspace has passed :meth:`_belongs_to_cluster`'s name-prefix
        check; node type and kind are only included if the workspace's
        catalog item + size flavour resolve to a configured node type
        (e.g. they won't for a workspace whose flavour was changed
        out-of-band after creation).
        """
        tags = {TAG_RAY_CLUSTER_NAME: self.cluster_name}
        node_type = self._node_type_for_workspace(workspace)
        if node_type is not None:
            tags[TAG_RAY_USER_NODE_TYPE] = node_type
            tags[TAG_RAY_NODE_KIND] = self._kind_for_node_type(node_type)
        return tags

    def _belongs_to_cluster(self, workspace: Mapping[str, Any]) -> bool:
        """Return whether a workspace's name marks it as managed by this cluster.

        A ``NodeProvider`` only ever operates within its own
        ``cluster_name`` namespace, so this is a prefix check against the
        naming convention applied at creation time (``_workspace_name_for``)
        rather than an attempt to parse an arbitrary cluster name back out.
        """
        name = workspace.get("name")
        return isinstance(name, str) and name.startswith(self._cluster_prefix)

    def _workspace_name_for(self, node_type: str) -> str:
        """Build the creation-time workspace name encoding cluster + node type.

        Used by cluster-membership checks (``_belongs_to_cluster``) and,
        eventually, node creation (section 4).
        """
        suffix = uuid.uuid4().hex[:8]
        sanitized_node_type = sanitize_name_component(node_type)
        base = f"{self._cluster_prefix}{sanitized_node_type}-"
        max_base_length = _WORKSPACE_NAME_MAX_LENGTH - len(suffix)
        if len(base) > max_base_length:
            base = base[:max_base_length]
        return f"{base}{suffix}"

    @staticmethod
    def _workspace_id(workspace: Mapping[str, Any]) -> str:
        workspace_id = workspace.get("id")
        if not isinstance(workspace_id, str) or not workspace_id:
            raise ValueError(f"SRC workspace response is missing a string id: {workspace!r}")
        return workspace_id

    @staticmethod
    def _workspace_status(workspace: Mapping[str, Any]) -> str:
        status = workspace.get("status")
        if not isinstance(status, str) or not status:
            raise ValueError(f"SRC workspace response is missing a string status: {workspace!r}")
        return status

    async def _list_workspaces(self) -> list[dict[str, Any]]:
        async with ResearchCloudClient.from_env() as client:
            co = await client.resolve_co(self.co_name)
            # Node types can use distinct catalog items per role (head vs.
            # worker), so no single catalog_item_name can be used to filter
            # server-side; cluster membership is instead established by the
            # workspace name prefix via _belongs_to_cluster().
            return await client.workspaces.list(
                co_id=co["id"],
                catalog_item_name="",
                application_type="Compute",
            )

    async def _get_workspace(self, workspace_id: str) -> dict[str, Any]:
        async with ResearchCloudClient.from_env() as client:
            return await client.workspaces.get(workspace_id)

    def _fetch_workspace(self, workspace_id: str) -> dict[str, Any] | None:
        """Fetch a single workspace, treating a 404 as "no longer exists" rather than an error."""
        try:
            return asyncio.run(self._get_workspace(workspace_id))
        except ApiError as exc:
            if exc.status_code == HTTP_NOT_FOUND:
                return None
            raise

    def _get_cached_or_fetch(self, workspace_id: str) -> dict[str, Any] | None:
        with self._workspaces_lock:
            workspace = self._workspaces.get(workspace_id)
        if workspace is not None:
            return workspace

        workspace = self._fetch_workspace(workspace_id)
        if workspace is None:
            return None
        self._workspace_id(workspace)
        self._workspace_status(workspace)
        with self._workspaces_lock:
            self._workspaces[workspace_id] = workspace
        return workspace

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        workspaces = asyncio.run(self._list_workspaces())
        current_workspaces: dict[str, dict[str, Any]] = {}
        for workspace in workspaces:
            workspace_id = self._workspace_id(workspace)
            self._workspace_status(workspace)
            current_workspaces[workspace_id] = workspace

        with self._workspaces_lock:
            self._workspaces = current_workspaces

        matching_node_ids = []
        for workspace_id, workspace in current_workspaces.items():
            if is_workspace_terminal_status(self._workspace_status(workspace)):
                continue
            if not self._belongs_to_cluster(workspace):
                continue
            if tag_filters:
                node_tags = self._node_tags_for_workspace(workspace)
                if any(node_tags.get(key) != value for key, value in tag_filters.items()):
                    continue
            matching_node_ids.append(workspace_id)
        return matching_node_ids

    def is_running(self, node_id: str) -> bool:
        workspace = self._get_cached_or_fetch(node_id)
        if workspace is None:
            return False
        return self._workspace_status(workspace) == "running"

    def is_terminated(self, node_id: str) -> bool:
        workspace = self._get_cached_or_fetch(node_id)
        if workspace is None:
            return True
        return is_workspace_terminal_status(self._workspace_status(workspace))

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Return this node's tags: identity tags (node kind, user node
        type, cluster name) derived from queryable workspace fields, merged
        with any mutable tags Ray has written via ``create_node``/
        ``set_node_tags`` and cached locally (see ``_NodeTagCache``, since
        SRC has nowhere to store them). Derived identity tags always win on
        key conflicts, since they reflect the workspace's actual state.
        """
        workspace = self._get_cached_or_fetch(node_id)
        if workspace is None:
            return {}
        tags = self._node_tag_cache.get(node_id)
        tags.update(self._node_tags_for_workspace(workspace))
        return tags

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Persist Ray's mutable tag writes to the local tag cache.

        Identity tags (node kind, user node type, cluster name) are derived
        from workspace fields and cannot be changed independently, so writes
        to those keys are cached but ultimately ignored by ``node_tags``
        (which always prefers the derived value). Everything else -- e.g.
        ``TAG_RAY_LAUNCH_CONFIG``, ``TAG_RAY_RUNTIME_CONFIG``,
        ``TAG_RAY_NODE_STATUS`` -- has no home in the SRC workspace itself,
        so it is kept in ``_NodeTagCache`` instead; see the module docstring
        for why this matters (losing ``TAG_RAY_LAUNCH_CONFIG`` makes every
        node look permanently out-of-date to Ray).
        """
        self._node_tag_cache.update(node_id, tags)

    async def _terminate_nodes(self, node_ids: list[str]) -> None:
        async with ResearchCloudClient.from_env() as client:
            for node_id in node_ids:
                try:
                    await client.workspaces.delete(node_id)
                except ApiError as exc:
                    if exc.status_code != HTTP_NOT_FOUND:
                        raise
                    logger.info("SRC workspace %r was already absent during termination", node_id)
                with self._workspaces_lock:
                    self._workspaces.pop(node_id, None)
        self._node_tag_cache.discard(node_ids)

    def terminate_node(self, node_id: str) -> None:
        """Delete one SRC workspace and forget its cached representation
        (including its locally cached mutable tags; see ``_NodeTagCache``).
        """
        asyncio.run(self._terminate_nodes([node_id]))

    def terminate_nodes(self, node_ids: list[str]) -> None:
        """Delete a batch of SRC workspaces using one client session, and
        forget their locally cached mutable tags (see ``_NodeTagCache``).
        """
        asyncio.run(self._terminate_nodes(node_ids))

    @staticmethod
    def _workspace_ip(workspace: Mapping[str, Any], *, private: bool) -> str:
        """Return a workspace's public or private address when assigned."""
        resource_meta = workspace.get("resource_meta")
        if not isinstance(resource_meta, Mapping):
            return ""
        field = "local_ip" if private else "ip"
        address = resource_meta.get(field)
        return address if isinstance(address, str) else ""

    def internal_ip(self, node_id: str) -> str:
        """Return the private address used for head-to-worker SSH."""
        workspace = self._get_cached_or_fetch(node_id)
        if workspace is None:
            return ""
        return self._workspace_ip(workspace, private=True)

    def external_ip(self, node_id: str) -> str:
        """Return the public address used by Ray's driver for initial SSH."""
        workspace = self._get_cached_or_fetch(node_id)
        if workspace is None:
            return ""
        return self._workspace_ip(workspace, private=False)
