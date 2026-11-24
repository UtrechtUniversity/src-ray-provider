from __future__ import annotations

import logging
import re
from typing import Any, Mapping

from researchcloud.client import ResearchCloudClient
from researchcloud.config import DEFAULT_CLOUD_NAME
from researchcloud.utils.flavours import match_size_flavour, parse_size_flavour

from .utils import deep_merge


DEFAULT_HEAD_CATALOG_ITEM_NAME = "Ray Head Node"
AUTOSCALING_HEAD_CATALOG_ITEM_NAME = "Ray Head Node (autoscaling)"
DEFAULT_WORKER_CATALOG_ITEM_NAME = "Ray Worker"
DEFAULT_OS_FLAVOUR_NAME = "Ubuntu 24.04"

logger = logging.getLogger(__name__)


def _resume_enabled(provider_config: Mapping[str, Any]) -> bool:
    reuse = provider_config.get("resume_paused_workspaces", False)
    if not isinstance(reuse, bool):
        raise ValueError("provider config 'resume_paused_workspaces' must be a boolean")
    return reuse


def _head_catalog_item_name(provider_config: Mapping[str, Any]) -> str:
    return AUTOSCALING_HEAD_CATALOG_ITEM_NAME if _resume_enabled(provider_config) else DEFAULT_HEAD_CATALOG_ITEM_NAME


def _size_flavour_resources(name: str) -> dict[str, int] | None:
    """Translate known flavour resources without inventing unknown CPU counts."""
    metadata = parse_size_flavour(name)
    cpu, gpu = metadata["cpu"], metadata["gpu"]
    if gpu is None:
        gpu_match = re.search(r"\b(\d+)\s+GPU\b", name, re.IGNORECASE)
        if gpu_match:
            gpu = int(gpu_match.group(1))

    resources = {}
    if cpu is not None and cpu > 0:
        resources["CPU"] = cpu
    if gpu is not None and gpu > 0:
        resources["GPU"] = gpu
    return resources or None


async def _catalog_node_types(
    provider_config: Mapping[str, Any],
    max_workers: int,
) -> tuple[dict[str, Any], str]:
    """Load CPU-only head and CPU/GPU worker types, selecting the smallest head."""
    co_name = provider_config.get("co_name")
    wallet_name = provider_config.get("wallet_name")
    if not isinstance(co_name, str) or not co_name.strip():
        raise ValueError("provider config must include a non-empty 'co_name' to load catalog flavours")
    if not isinstance(wallet_name, str) or not wallet_name.strip():
        raise ValueError("provider config must include a non-empty 'wallet_name' to load catalog flavours")

    cloud_name = provider_config.get("cloud_name", DEFAULT_CLOUD_NAME)
    os_flavour_name = provider_config.get("os_flavour_name", DEFAULT_OS_FLAVOUR_NAME)
    generated: dict[str, Any] = {}
    generated_head_types: list[tuple[int, str, str]] = []

    async with ResearchCloudClient.from_env() as client:
        co = await client.resolve_co(co_name)
        wallet = await client.resolve_wallet(wallet_name)
        budgets = wallet.get("budgets")
        if not isinstance(budgets, list) or not budgets or not isinstance(budgets[0], Mapping):
            raise ValueError(f"ResearchCloud wallet {wallet_name!r} has no usable budget")
        products = budgets[0].get("products")
        if not isinstance(products, list) or not products:
            raise ValueError(f"ResearchCloud wallet {wallet_name!r} has no products available")

        for role, catalog_item_name in (
            ("head", _head_catalog_item_name(provider_config)),
            ("worker", DEFAULT_WORKER_CATALOG_ITEM_NAME),
        ):
            catalog_item = await client.resolve_catalog_item(catalog_item_name, co["id"], products)
            offering, _, _ = await client.resolve_offering_and_flavours(
                catalog_item,
                co["id"],
                products,
                cloud_name,
                os_flavour_name,
                None,
            )
            size_flavours = [
                flavour for flavour in offering.get("flavours", []) if flavour.get("category") == "size"
            ]
            valid_count = 0
            for flavour in size_flavours:
                flavour_name = flavour.get("name")
                if not isinstance(flavour_name, str) or not flavour_name.strip():
                    logger.warning("Skipping unnamed size flavour in %s catalog item", catalog_item_name)
                    continue
                resources = _size_flavour_resources(flavour_name)
                if resources is None:
                    logger.warning(
                        "Skipping size flavour %r from %s because no CPU or GPU count could be parsed",
                        flavour_name,
                        catalog_item_name,
                    )
                    continue
                if role == "head" and "GPU" in resources:
                    continue

                node_type = f"{role}_{re.sub(r'[^a-z0-9]+', '_', flavour_name.lower()).strip('_')}"
                if node_type in generated:
                    raise ValueError(
                        f"size flavour names in {catalog_item_name!r} produce duplicate Ray node type "
                        f"name {node_type!r}"
                    )
                generated[node_type] = {
                    "min_workers": 0,
                    "max_workers": 0 if role == "head" else max_workers,
                    "resources": resources,
                    "node_config": {
                        "catalog_item_name": catalog_item_name,
                        "size_flavour_name": flavour_name,
                    },
                }
                valid_count += 1
                if role == "head":
                    generated_head_types.append((resources["CPU"], flavour_name, node_type))
            if not valid_count:
                resource_types = "CPU-only" if role == "head" else "CPU or GPU"
                raise ValueError(
                    f"No usable {resource_types} size flavours found for {catalog_item_name!r} on cloud {cloud_name!r}"
                )

    smallest_head_type = min(generated_head_types)[2]
    generated["head"] = generated.pop(smallest_head_type)
    return generated, "head"


def _catalog_flavours_by_item(generated: Mapping[str, Any]) -> dict[str, list[dict[str, str]]]:
    """Index generated flavour names in the shape expected by the SDK matcher."""
    flavours: dict[str, list[dict[str, str]]] = {}
    for node in generated.values():
        config = node["node_config"]
        flavours.setdefault(config["catalog_item_name"], []).append(
            {"category": "size", "name": config["size_flavour_name"]}
        )
    return flavours


def _node_catalog_flavour(
    name: str,
    config: Mapping[str, Any],
    head_node_type: str,
    flavours_by_item: Mapping[str, list[dict[str, str]]],
    head_catalog_item_name: str,
) -> tuple[str, str] | None:
    """Resolve a node's catalog/flavour identity, including CPU/GPU selectors."""
    node_config = config.get("node_config", {})
    if not isinstance(node_config, Mapping):
        return None

    catalog_name = node_config.get(
        "catalog_item_name",
        head_catalog_item_name if name == head_node_type else DEFAULT_WORKER_CATALOG_ITEM_NAME,
    )
    flavour_name = node_config.get("size_flavour_name")
    if (
        flavour_name is None
        and catalog_name in flavours_by_item
        and (node_config.get("num_cpu") is not None or node_config.get("num_gpu") is not None)
    ):
        flavour_name = match_size_flavour(
            flavours_by_item[catalog_name],
            num_cpu=node_config.get("num_cpu"),
            num_gpu=node_config.get("num_gpu"),
            gpu_type=node_config.get("gpu_type"),
        )["name"]
    if not isinstance(catalog_name, str) or not isinstance(flavour_name, str):
        return None
    return catalog_name, flavour_name


def _merge_node_config_overrides(
    available: dict[str, Any],
    user_node_types: Mapping[str, Any],
) -> None:
    """When sizing is overridden, discard inherited selectors to avoid conflicting strategies."""
    sizing_keys = ("size_flavour_name", "num_cpu", "num_gpu", "gpu_type")
    for name, config in user_node_types.items():
        overrides = config.get("node_config", {}) if isinstance(config, Mapping) else {}
        if not isinstance(overrides, Mapping) or not any(key in overrides for key in sizing_keys[:3]):
            continue
        node_config = available[name].get("node_config")
        if isinstance(node_config, dict):
            for key in sizing_keys:
                if key not in overrides:
                    node_config.pop(key, None)


def _merge_catalog_node_types(
    generated: dict[str, Any],
    user_node_types: Mapping[str, Any],
    default_head_node_type: str,
    head_node_type: str | None,
    head_catalog_item_name: str,
) -> tuple[dict[str, Any], str]:
    """Merge user overrides, deduplicate SRC identities, and return the default head type.

    Ray node types must resolve to unique (catalog item, size flavour) pairs
    because workspace discovery reverse-maps that pair to one Ray type.
    Explicit YAML entries take precedence over generated entries with a matching pair.
    """
    available = deep_merge(generated, dict(user_node_types))
    _merge_node_config_overrides(available, user_node_types)
    selected_head = default_head_node_type if head_node_type is None else head_node_type
    flavours_by_item = _catalog_flavours_by_item(generated)
    configured_pairs: dict[tuple[str, str], str] = {}

    for name, config in list(available.items()):
        pair = _node_catalog_flavour(name, config, selected_head, flavours_by_item, head_catalog_item_name)
        if pair is None:
            continue

        previous = configured_pairs.get(pair)
        if previous is not None and name in user_node_types and previous not in user_node_types:
            available.pop(previous)
            configured_pairs[pair] = name
        elif previous is not None and previous in user_node_types and name not in user_node_types:
            available.pop(name)
        else:
            configured_pairs[pair] = name

    if head_node_type is None:
        head_config = generated[default_head_node_type]["node_config"]
        head_pair = (head_config["catalog_item_name"], head_config["size_flavour_name"])
        head_node_type = configured_pairs.get(head_pair, default_head_node_type)
    return available, head_node_type


def _provider_node_types(available_node_types: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    keys = ("size_flavour_name", "num_cpu", "num_gpu", "gpu_type", "catalog_item_name", "ray_packages")
    node_types = {}
    for name, config in available_node_types.items():
        node_config = config.get("node_config", {}) if isinstance(config, Mapping) else {}
        node_types[name] = {key: node_config[key] for key in keys if key in node_config}
    return node_types
