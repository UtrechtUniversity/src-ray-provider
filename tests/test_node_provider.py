from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_USER_NODE_TYPE,
)
from researchcloud.config import DEFAULT_CLOUD_NAME
from researchcloud.errors import ApiError
from src_ray_provider.node_provider import (
    DEFAULT_HEAD_CATALOG_ITEM_NAME,
    DEFAULT_OS_FLAVOUR_NAME,
    DEFAULT_WORKER_CATALOG_ITEM_NAME,
    DEFAULT_WORKSPACE_CREATION_TIMEOUT,
    ResearchCloudNodeProvider,
    sanitize_name_component,
)


def _provider_config(**overrides) -> dict:
    config = {
        "co_name": "Example CO",
        "wallet_name": "Example Wallet",
        "head_node_type": "head",
        "node_types": {
            "head": {"size_flavour_name": "2 Core - 8 GB"},
            "worker": {"size_flavour_name": "4 Core - 16 GB"},
        },
    }
    config.update(overrides)
    return config


CLUSTER_NAME = "test-cluster"
CLUSTER_PREFIX = f"ray-{CLUSTER_NAME}-"


def test_provider_configuration_uses_defaults():
    provider = ResearchCloudNodeProvider(_provider_config(), CLUSTER_NAME)

    assert provider.cloud_name == DEFAULT_CLOUD_NAME
    assert provider.os_flavour_name == DEFAULT_OS_FLAVOUR_NAME
    assert provider.network_name_hint is None
    assert provider.workspace_creation_timeout == DEFAULT_WORKSPACE_CREATION_TIMEOUT
    assert provider._workspace_creation_options("head") == {
        "catalog_item_name": DEFAULT_HEAD_CATALOG_ITEM_NAME,
        "cloud_name": DEFAULT_CLOUD_NAME,
        "os_flavour_name": DEFAULT_OS_FLAVOUR_NAME,
        "size_flavour_name": "2 Core - 8 GB",
        "use_private_network": True,
        "network_name_hint": None,
        "optional_parameters": {"ray_public_key": "", "ray_do_setup": "false"},
    }
    assert provider._workspace_creation_options("worker") == {
        "catalog_item_name": DEFAULT_WORKER_CATALOG_ITEM_NAME,
        "cloud_name": DEFAULT_CLOUD_NAME,
        "os_flavour_name": DEFAULT_OS_FLAVOUR_NAME,
        "size_flavour_name": "4 Core - 16 GB",
        "use_private_network": True,
        "network_name_hint": None,
        "optional_parameters": {"ray_public_key": "", "ray_do_setup": "false"},
    }


def test_provider_configuration_allows_overrides():
    provider = ResearchCloudNodeProvider(
        _provider_config(
            cloud_name="Custom Cloud",
            os_flavour_name="Ubuntu 22.04",
            network_name_hint="ray-private",
            node_types={
                "head": {"size_flavour_name": "2 Core - 8 GB", "catalog_item_name": "Custom Ray Head"},
                "worker": {"size_flavour_name": "8 Core - 32 GB", "catalog_item_name": "Custom Ray Worker"},
            },
        ),
        CLUSTER_NAME,
    )

    assert provider._workspace_creation_options("worker") == {
        "catalog_item_name": "Custom Ray Worker",
        "cloud_name": "Custom Cloud",
        "os_flavour_name": "Ubuntu 22.04",
        "size_flavour_name": "8 Core - 32 GB",
        "use_private_network": True,
        "network_name_hint": "ray-private",
        "optional_parameters": {"ray_public_key": "", "ray_do_setup": "false"},
    }


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True, "30"])
def test_workspace_creation_timeout_must_be_a_positive_finite_number(timeout):
    with pytest.raises(ValueError, match="workspace_creation_timeout"):
        ResearchCloudNodeProvider(_provider_config(workspace_creation_timeout=timeout), CLUSTER_NAME)


@pytest.mark.parametrize("key", ["co_name", "wallet_name", "head_node_type"])
def test_required_provider_configuration_must_be_non_empty(key):
    with pytest.raises(ValueError, match=key):
        ResearchCloudNodeProvider(_provider_config(**{key: "  "}), CLUSTER_NAME)


def test_exactly_one_sizing_strategy_is_required_for_each_node_type():
    config = _provider_config(node_types={"head": {}, "worker": {"size_flavour_name": "4 Core - 16 GB"}})
    with pytest.raises(ValueError, match="node_types\\['head'\\]"):
        ResearchCloudNodeProvider(config, CLUSTER_NAME)


def test_at_most_one_sizing_strategy_is_allowed_for_each_node_type():
    config = _provider_config(
        node_types={
            "head": {"size_flavour_name": "2 Core - 8 GB", "num_cpu": 2},
            "worker": {"size_flavour_name": "4 Core - 16 GB"},
        }
    )
    with pytest.raises(ValueError, match="node_types\\['head'\\]"):
        ResearchCloudNodeProvider(config, CLUSTER_NAME)


def test_node_types_mapping_is_required():
    config = _provider_config()
    del config["node_types"]
    with pytest.raises(ValueError, match="node_types"):
        ResearchCloudNodeProvider(config, CLUSTER_NAME)


def test_head_node_type_must_be_a_key_in_node_types():
    with pytest.raises(ValueError, match="head_node_type"):
        ResearchCloudNodeProvider(_provider_config(head_node_type="no-such-type"), CLUSTER_NAME)


def test_node_types_must_resolve_to_distinct_catalog_item_and_flavour_pairs():
    config = _provider_config(
        node_types={
            "head": {"size_flavour_name": "2 Core - 8 GB", "catalog_item_name": "Shared Catalog Item"},
            "worker": {"size_flavour_name": "2 Core - 8 GB", "catalog_item_name": "Shared Catalog Item"},
        }
    )
    with pytest.raises(ValueError, match="same SRC"):
        ResearchCloudNodeProvider(config, CLUSTER_NAME)


def test_catalog_item_name_defaults_by_role():
    provider = ResearchCloudNodeProvider(_provider_config(), CLUSTER_NAME)

    assert provider._node_type_configs["head"]["catalog_item_name"] == DEFAULT_HEAD_CATALOG_ITEM_NAME
    assert provider._node_type_configs["worker"]["catalog_item_name"] == DEFAULT_WORKER_CATALOG_ITEM_NAME


class TestBootstrapConfig:
    """``bootstrap_config`` lets cluster.yaml authors skip manually duplicating
    ``head_node_type``/``node_types`` under ``provider:`` (see module
    docstring); it derives them from the full cluster config instead.
    """

    def test_copies_head_node_type_from_cluster_config_top_level(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "head_node_type": "head",
            "available_node_types": {"head": {"node_config": {"size_flavour_name": "2 Core - 8 GB"}}},
            "provider": {"co_name": "Example CO", "wallet_name": "Example Wallet"},
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["provider"]["head_node_type"] == "head"

    def test_adds_provider_installation_to_head_setup_commands(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "head_node_type": "head",
            "available_node_types": {},
            "provider": {"co_name": "Example CO", "wallet_name": "Example Wallet"},
            "head_setup_commands": ["echo custom setup"],
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["head_setup_commands"] == [
            "echo custom setup",
            'python3 -m pip install --upgrade "src-ray-provider @ '
            "git+https://github.com/UtrechtUniversity/src-ray-provider.git\"",
        ]

    def test_head_setup_install_command_is_added_only_once(self):
        cluster_config = {"cluster_name": CLUSTER_NAME, "head_setup_commands": []}

        ResearchCloudNodeProvider.bootstrap_config(cluster_config)
        ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert len(cluster_config["head_setup_commands"]) == 1

    def test_rejects_non_list_head_setup_commands(self):
        with pytest.raises(ValueError, match="head_setup_commands.*list"):
            ResearchCloudNodeProvider.bootstrap_config({"head_setup_commands": "echo setup"})

    def test_derives_node_types_from_available_node_types_node_config(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {"size_flavour_name": "2 Core - 8 GB"}},
                "worker": {
                    "node_config": {
                        "size_flavour_name": "4 Core - 16 GB",
                        "catalog_item_name": "Custom Worker Item",
                    }
                },
            },
            "provider": {"co_name": "Example CO", "wallet_name": "Example Wallet"},
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["provider"]["node_types"] == {
            "head": {"size_flavour_name": "2 Core - 8 GB"},
            "worker": {"size_flavour_name": "4 Core - 16 GB", "catalog_item_name": "Custom Worker Item"},
        }

    def test_does_not_override_an_explicit_provider_head_node_type(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "head_node_type": "head",
            "available_node_types": {"head": {"node_config": {"size_flavour_name": "2 Core - 8 GB"}}},
            "provider": {
                "co_name": "Example CO",
                "wallet_name": "Example Wallet",
                "head_node_type": "explicit-override",
            },
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["provider"]["head_node_type"] == "explicit-override"

    def test_does_not_override_an_explicit_provider_node_types(self):
        explicit_node_types = {"head": {"size_flavour_name": "explicit-override"}}
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "head_node_type": "head",
            "available_node_types": {"head": {"node_config": {"size_flavour_name": "2 Core - 8 GB"}}},
            "provider": {
                "co_name": "Example CO",
                "wallet_name": "Example Wallet",
                "node_types": explicit_node_types,
            },
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["provider"]["node_types"] == explicit_node_types

    def test_bootstrapped_config_constructs_a_working_provider(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "head_node_type": "head",
            "available_node_types": {
                "head": {"node_config": {"size_flavour_name": "2 Core - 8 GB"}},
                "worker": {"node_config": {"size_flavour_name": "4 Core - 16 GB"}},
            },
            "provider": {"co_name": "Example CO", "wallet_name": "Example Wallet"},
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)
        provider = ResearchCloudNodeProvider(result["provider"], CLUSTER_NAME)

        assert provider.head_node_type == "head"
        assert provider._node_type_configs["worker"]["size_flavour_name"] == "4 Core - 16 GB"

    def test_rejects_ssh_public_key_without_ssh_private_key(self):
        """Ray only syncs auth.ssh_private_key onto the head node (so its
        autoscaler monitor can SSH into newly created workers) when it is
        explicitly set. Without it, workers are created successfully but
        every SSH attempt into them fails with "permission denied" -- fail
        fast here instead.
        """
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "available_node_types": {},
            "auth": {"ssh_user": "ray", "ssh_public_key": "~/.ssh/id_rsa.pub"},
        }

        with pytest.raises(ValueError, match="ssh_public_key.*ssh_private_key.*must both be set"):
            ResearchCloudNodeProvider.bootstrap_config(cluster_config)

    def test_rejects_ssh_private_key_without_ssh_public_key(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "available_node_types": {},
            "auth": {"ssh_user": "ray", "ssh_private_key": "~/.ssh/id_rsa"},
        }

        with pytest.raises(ValueError, match="ssh_public_key.*ssh_private_key.*must both be set"):
            ResearchCloudNodeProvider.bootstrap_config(cluster_config)

    def test_derives_ray_public_key_from_ssh_public_key_file_contents(self, tmp_path):
        key_path = tmp_path / "id_rsa.pub"
        key_path.write_text("ssh-ed25519 AAAAfake user@example\n", encoding="utf-8")
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "available_node_types": {},
            "auth": {
                "ssh_user": "ray",
                "ssh_public_key": str(key_path),
                "ssh_private_key": str(tmp_path / "id_rsa"),
            },
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["provider"]["ray_public_key"] == "ssh-ed25519 AAAAfake user@example"

    def test_derives_ray_public_key_from_literal_string_when_not_a_file(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "available_node_types": {},
            "auth": {
                "ssh_user": "ray",
                "ssh_public_key": "ssh-ed25519 AAAAfake user@example",
                "ssh_private_key": "~/.ssh/id_rsa",
            },
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["provider"]["ray_public_key"] == "ssh-ed25519 AAAAfake user@example"

    def test_does_not_override_an_explicit_provider_ray_public_key(self):
        cluster_config = {
            "cluster_name": CLUSTER_NAME,
            "available_node_types": {},
            "auth": {
                "ssh_user": "ray",
                "ssh_public_key": "ssh-ed25519 AAAAfake user@example",
                "ssh_private_key": "~/.ssh/id_rsa",
            },
            "provider": {"ray_public_key": "explicit-override"},
        }

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        assert result["provider"]["ray_public_key"] == "explicit-override"

    def test_generates_and_reuses_a_keypair_when_auth_has_no_keys(self, tmp_path, monkeypatch):
        """Without any auth.ssh_public_key/ssh_private_key, nothing in Ray
        generates SSH credentials for the "external" provider type -- so
        without this fallback, SRC workspaces would be created with no
        authorized key and nothing could SSH into them (see module/method
        docstrings for the AWS/vSphere precedent this mirrors).
        """
        monkeypatch.setattr(
            "src_ray_provider.node_provider.SSH_KEY_CACHE_DIR", tmp_path / "ssh_keys"
        )
        cluster_config = {"cluster_name": CLUSTER_NAME, "available_node_types": {}}

        result = ResearchCloudNodeProvider.bootstrap_config(cluster_config)

        private_key_path = Path(result["auth"]["ssh_private_key"])
        public_key_path = Path(result["auth"]["ssh_public_key"])
        assert private_key_path.is_file()
        assert public_key_path.is_file()
        assert result["provider"]["ray_public_key"] == public_key_path.read_text(encoding="utf-8").strip()
        assert result["provider"]["ray_public_key"].startswith("ssh-ed25519 ")

        # A second bootstrap_config call (e.g. a repeat `ray up`) must reuse
        # the same keypair rather than generating a new, unrecognized one.
        second_cluster_config = {"cluster_name": CLUSTER_NAME, "available_node_types": {}}
        second_result = ResearchCloudNodeProvider.bootstrap_config(second_cluster_config)

        assert second_result["auth"]["ssh_private_key"] == str(private_key_path)
        assert second_result["provider"]["ray_public_key"] == result["provider"]["ray_public_key"]

    def test_rejects_generating_a_keypair_without_a_cluster_name(self):
        cluster_config = {"available_node_types": {}}

        with pytest.raises(ValueError, match="cluster_name.*non-empty string"):
            ResearchCloudNodeProvider.bootstrap_config(cluster_config)

    def test_rejects_non_mapping_auth(self):
        cluster_config = {"cluster_name": CLUSTER_NAME, "available_node_types": {}, "auth": "not-a-mapping"}

        with pytest.raises(ValueError, match="'auth' must be a mapping"):
            ResearchCloudNodeProvider.bootstrap_config(cluster_config)



class TestSanitizeNameComponent:
    def test_replaces_disallowed_characters_with_dashes(self):
        assert sanitize_name_component("my.cluster/name") == "my-cluster-name"

    def test_strips_leading_and_trailing_dashes(self):
        assert sanitize_name_component("  --cluster--  ") == "cluster"

    def test_raises_for_a_value_with_no_valid_characters(self):
        with pytest.raises(ValueError, match="cannot derive"):
            sanitize_name_component("...")


class TestWorkspaceNameFor:
    def test_embeds_cluster_prefix_and_node_type(self):
        provider = ResearchCloudNodeProvider(_provider_config(), CLUSTER_NAME)

        name = provider._workspace_name_for("worker")

        assert name.startswith(f"{CLUSTER_PREFIX}worker-")
        assert len(name) <= 100


class _FakeClient:
    """A minimal stand-in for `ResearchCloudClient` used as an async context manager."""

    def __init__(
        self,
        *,
        co: dict,
        workspaces: list[dict],
        get_by_id: dict[str, dict] | None = None,
        wallet: dict | None = None,
        catalog_item: dict | None = None,
        offering: dict | None = None,
    ):
        self.resolve_co = AsyncMock(return_value=co)
        self.resolve_wallet = AsyncMock(return_value=wallet)
        self.resolve_catalog_item = AsyncMock(return_value=catalog_item)
        self.resolve_offering_and_flavours = AsyncMock(return_value=(offering, None, None))
        self.workspaces = _FakeWorkspacesService(workspaces, get_by_id or {})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return None


class _FakeWorkspacesService:
    def __init__(self, workspaces: list[dict], get_by_id: dict[str, dict]):
        self.list = AsyncMock(return_value=workspaces)
        self.build_create_payload_from_names = AsyncMock()
        self.create = AsyncMock()
        self.delete = AsyncMock()
        self._get_by_id = get_by_id

    async def get(self, workspace_id: str) -> dict:
        if workspace_id not in self._get_by_id:
            raise ApiError(404, f"workspaces/{workspace_id}/", {"detail": "not found"})
        return self._get_by_id[workspace_id]


def _provider(**overrides) -> ResearchCloudNodeProvider:
    return ResearchCloudNodeProvider(_provider_config(**overrides), CLUSTER_NAME)


def _patched_from_env(fake_client: _FakeClient):
    return patch("src_ray_provider.node_provider.ResearchCloudClient.from_env", return_value=fake_client)


class TestNumCpuGpuSizing:
    def test_resolves_size_flavour_name_from_num_cpu(self):
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[],
            wallet={"budgets": [{"products": ["prod-1"]}]},
            catalog_item={"name": DEFAULT_WORKER_CATALOG_ITEM_NAME},
            offering={
                "flavours": [
                    {"name": "2 Core - 8 GB", "category": "size"},
                    {"name": "4 Core - 16 GB", "category": "size"},
                ]
            },
        )
        config = _provider_config(node_types={"head": {"size_flavour_name": "2 Core - 8 GB"}, "worker": {"num_cpu": 4}})
        with _patched_from_env(fake_client):
            provider = ResearchCloudNodeProvider(config, CLUSTER_NAME)

        assert provider._node_type_configs["worker"]["size_flavour_name"] == "4 Core - 16 GB"
        fake_client.resolve_co.assert_awaited_once_with("Example CO")
        fake_client.resolve_wallet.assert_awaited_once_with("Example Wallet")
        fake_client.resolve_catalog_item.assert_awaited_once()
        fake_client.resolve_offering_and_flavours.assert_awaited_once()

    def test_resolves_size_flavour_name_from_num_gpu_and_gpu_type(self):
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[],
            wallet={"budgets": [{"products": ["prod-1"]}]},
            catalog_item={"name": DEFAULT_WORKER_CATALOG_ITEM_NAME},
            offering={"flavours": [{"name": "4 Core - 16 GB - 1x A100", "category": "size"}]},
        )
        config = _provider_config(
            node_types={
                "head": {"size_flavour_name": "2 Core - 8 GB"},
                "worker": {"num_gpu": 1, "gpu_type": "A100"},
            }
        )
        with _patched_from_env(fake_client):
            provider = ResearchCloudNodeProvider(config, CLUSTER_NAME)

        assert provider._node_type_configs["worker"]["size_flavour_name"] == "4 Core - 16 GB - 1x A100"

    def test_does_not_contact_the_api_when_size_flavour_name_is_given_for_every_node_type(self):
        with patch("src_ray_provider.node_provider.ResearchCloudClient.from_env") as mock_from_env:
            _provider()

        mock_from_env.assert_not_called()

    def test_resolves_catalog_item_once_per_distinct_catalog_item(self):
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[],
            wallet={"budgets": [{"products": ["prod-1"]}]},
            catalog_item={"name": "Shared Catalog Item"},
            offering={
                "flavours": [
                    {"name": "2 Core - 8 GB", "category": "size"},
                    {"name": "4 Core - 16 GB", "category": "size"},
                ]
            },
        )
        config = _provider_config(
            node_types={
                "head": {"num_cpu": 2, "catalog_item_name": "Shared Catalog Item"},
                "worker": {"num_cpu": 4, "catalog_item_name": "Shared Catalog Item"},
            }
        )
        with _patched_from_env(fake_client):
            ResearchCloudNodeProvider(config, CLUSTER_NAME)

        fake_client.resolve_catalog_item.assert_awaited_once()


def _workspace(
    *,
    id: str,
    status: str,
    name: str,
    catalog_item_name: str | None = None,
    flavor_name: str | None = None,
    ip: str | None = None,
    local_ip: str | None = None,
) -> dict:
    workspace: dict = {
        "id": id,
        "status": status,
        "name": name,
        "meta": {},
        "resource_meta": {},
    }
    if catalog_item_name is not None:
        workspace["meta"]["application_name"] = catalog_item_name
    if flavor_name is not None:
        # Mirrors the real SRC API shape: the catalog size flavour's
        # display name is listed in meta.flavours (category "size"), not
        # resource_meta.flavor_name (an unrelated infrastructure-level
        # slug, e.g. "hpc-1core-8gb-20gb", that node type matching must
        # not rely on).
        workspace["meta"]["flavours"] = [{"category": "size", "name": flavor_name}]
        workspace["resource_meta"]["flavor_name"] = "infra-slug-unrelated-to-catalog-name"
    if ip is not None:
        workspace["resource_meta"]["ip"] = ip
    if local_ip is not None:
        workspace["resource_meta"]["local_ip"] = local_ip
    return workspace


def _worker_workspace(id: str, status: str = "running", name: str | None = None) -> dict:
    return _workspace(
        id=id,
        status=status,
        name=name or f"{CLUSTER_PREFIX}worker-{id}",
        catalog_item_name=DEFAULT_WORKER_CATALOG_ITEM_NAME,
        flavor_name="4 Core - 16 GB",
    )


def _head_workspace(id: str, status: str = "running", name: str | None = None) -> dict:
    return _workspace(
        id=id,
        status=status,
        name=name or f"{CLUSTER_PREFIX}head-{id}",
        catalog_item_name=DEFAULT_HEAD_CATALOG_ITEM_NAME,
        flavor_name="2 Core - 8 GB",
    )


class TestNonTerminatedNodes:
    def test_returns_only_non_terminal_workspaces_when_no_tag_filters(self):
        provider = _provider()
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[
                _worker_workspace("ws-running", status="running"),
                _worker_workspace("ws-deleted", status="deleted"),
            ],
        )

        with _patched_from_env(fake_client):
            assert provider.non_terminated_nodes({}) == ["ws-running"]

    def test_excludes_workspaces_belonging_to_a_different_cluster(self):
        provider = _provider()
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[
                _worker_workspace("ws-ours"),
                _worker_workspace("ws-other-cluster", name="ray-other-cluster-worker-1"),
            ],
        )

        with _patched_from_env(fake_client):
            assert provider.non_terminated_nodes({}) == ["ws-ours"]

    def test_filters_by_derived_node_type_tag(self):
        provider = _provider()
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[_worker_workspace("ws-worker"), _head_workspace("ws-head")],
        )

        with _patched_from_env(fake_client):
            result = provider.non_terminated_nodes({TAG_RAY_USER_NODE_TYPE: "worker"})

        assert result == ["ws-worker"]

    def test_filters_by_derived_node_kind_tag(self):
        provider = _provider()
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[_worker_workspace("ws-worker"), _head_workspace("ws-head")],
        )

        with _patched_from_env(fake_client):
            result = provider.non_terminated_nodes({TAG_RAY_NODE_KIND: NODE_KIND_HEAD})

        assert result == ["ws-head"]

    def test_filters_by_derived_cluster_name_tag(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[_worker_workspace("ws-worker")])

        with _patched_from_env(fake_client):
            matching = provider.non_terminated_nodes({TAG_RAY_CLUSTER_NAME: CLUSTER_NAME})
            non_matching = provider.non_terminated_nodes({TAG_RAY_CLUSTER_NAME: "some-other-cluster"})

        assert matching == ["ws-worker"]
        assert non_matching == []

    def test_tag_filters_exclude_workspaces_whose_flavour_does_not_match_any_node_type(self):
        provider = _provider()
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[
                _workspace(
                    id="ws-unknown-flavour",
                    status="running",
                    name=f"{CLUSTER_PREFIX}worker-unknown",
                    catalog_item_name=DEFAULT_WORKER_CATALOG_ITEM_NAME,
                    flavor_name="some-unconfigured-flavour",
                )
            ],
        )

        with _patched_from_env(fake_client):
            assert provider.non_terminated_nodes({TAG_RAY_USER_NODE_TYPE: "worker"}) == []


class TestNodeTags:
    def test_returns_derived_tags_for_a_cached_head_workspace(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[_head_workspace("ws-1")])

        with _patched_from_env(fake_client):
            provider.non_terminated_nodes({})
            assert provider.node_tags("ws-1") == {
                TAG_RAY_CLUSTER_NAME: CLUSTER_NAME,
                TAG_RAY_USER_NODE_TYPE: "head",
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            }

    def test_returns_derived_tags_for_a_cached_worker_workspace(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[_worker_workspace("ws-1")])

        with _patched_from_env(fake_client):
            provider.non_terminated_nodes({})
            assert provider.node_tags("ws-1") == {
                TAG_RAY_CLUSTER_NAME: CLUSTER_NAME,
                TAG_RAY_USER_NODE_TYPE: "worker",
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
            }

    def test_returns_only_cluster_name_tag_when_flavour_does_not_match_any_node_type(self):
        provider = _provider()
        workspace = _workspace(
            id="ws-1",
            status="running",
            name=f"{CLUSTER_PREFIX}worker-1",
            catalog_item_name=DEFAULT_WORKER_CATALOG_ITEM_NAME,
            flavor_name="some-unconfigured-flavour",
        )
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[workspace])

        with _patched_from_env(fake_client):
            provider.non_terminated_nodes({})
            assert provider.node_tags("ws-1") == {TAG_RAY_CLUSTER_NAME: CLUSTER_NAME}

    def test_returns_empty_dict_for_a_workspace_that_no_longer_exists(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={})

        with _patched_from_env(fake_client):
            assert provider.node_tags("ws-missing") == {}


class TestNodeTagCache:
    """Covers persisting mutable Ray tags (e.g. TAG_RAY_LAUNCH_CONFIG) that
    SRC workspaces have nowhere to store, since losing them makes every
    node look permanently out-of-date to Ray's `_should_create_new_head`
    and causes `ray up` to destroy and recreate an otherwise healthy node
    on every invocation.
    """

    def test_set_node_tags_persists_and_is_merged_into_node_tags(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[_head_workspace("ws-1")])

        with _patched_from_env(fake_client):
            provider.non_terminated_nodes({})
            provider.set_node_tags("ws-1", {TAG_RAY_LAUNCH_CONFIG: "hash-123"})

            assert provider.node_tags("ws-1") == {
                TAG_RAY_CLUSTER_NAME: CLUSTER_NAME,
                TAG_RAY_USER_NODE_TYPE: "head",
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                TAG_RAY_LAUNCH_CONFIG: "hash-123",
            }

    def test_derived_identity_tags_win_over_a_stale_cached_value(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[_head_workspace("ws-1")])

        with _patched_from_env(fake_client):
            provider.non_terminated_nodes({})
            # A cached write can never override a derived identity tag, even
            # if a caller tried to set a conflicting value for it.
            provider.set_node_tags("ws-1", {TAG_RAY_USER_NODE_TYPE: "worker"})

            assert provider.node_tags("ws-1")[TAG_RAY_USER_NODE_TYPE] == "head"

    def test_cached_tags_survive_a_new_provider_instance_for_the_same_cluster(self):
        """Simulates a separate `ray` command invocation (e.g. a later `ray
        up`) reusing the same local cache file on the same machine.
        """
        workspace = _head_workspace("ws-1")
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[workspace], get_by_id={"ws-1": workspace})

        provider_a = _provider()
        with _patched_from_env(fake_client):
            provider_a.non_terminated_nodes({})
            provider_a.set_node_tags("ws-1", {TAG_RAY_LAUNCH_CONFIG: "hash-123"})

        provider_b = _provider()
        with _patched_from_env(fake_client):
            assert provider_b.node_tags("ws-1")[TAG_RAY_LAUNCH_CONFIG] == "hash-123"

    def test_create_node_caches_the_tags_ray_passed_in(self):
        provider = _provider()
        created_workspace = {"id": "ws-1", "name": "node-1", "status": "running"}
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={"ws-1": created_workspace})
        fake_client.workspaces.build_create_payload_from_names.side_effect = (
            lambda **kwargs: SimpleNamespace(payload={"name": kwargs["workspace_name"]})
        )
        fake_client.workspaces.create.side_effect = [created_workspace]
        tags = {
            TAG_RAY_USER_NODE_TYPE: "head",
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_LAUNCH_CONFIG: "hash-abc",
        }

        with _patched_from_env(fake_client):
            provider.create_node({}, tags, 1)
            assert provider.node_tags("ws-1")[TAG_RAY_LAUNCH_CONFIG] == "hash-abc"

    def test_terminate_node_discards_its_cached_tags(self):
        provider = _provider()
        workspace = _head_workspace("ws-1")
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[workspace], get_by_id={"ws-1": workspace})

        with _patched_from_env(fake_client):
            provider.non_terminated_nodes({})
            provider.set_node_tags("ws-1", {TAG_RAY_LAUNCH_CONFIG: "hash-123"})
            provider.terminate_node("ws-1")

        fake_client_after = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={})
        with _patched_from_env(fake_client_after):
            assert provider.node_tags("ws-1") == {}


class TestNodeAddresses:
    def test_returns_public_and_private_addresses_from_resource_metadata(self):
        provider = _provider()
        workspace = _worker_workspace("ws-1")
        workspace["resource_meta"].update({"ip": "145.38.195.241", "local_ip": "10.10.10.93"})
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={"ws-1": workspace})

        with _patched_from_env(fake_client):
            assert provider.external_ip("ws-1") == "145.38.195.241"
            assert provider.internal_ip("ws-1") == "10.10.10.93"

    def test_returns_empty_string_when_addresses_are_not_assigned(self):
        provider = _provider()
        fake_client = _FakeClient(
            co={"id": "co-1"},
            workspaces=[],
            get_by_id={"ws-1": _worker_workspace("ws-1")},
        )

        with _patched_from_env(fake_client):
            assert provider.external_ip("ws-1") == ""
            assert provider.internal_ip("ws-1") == ""

    def test_returns_empty_string_for_a_missing_workspace(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={})

        with _patched_from_env(fake_client):
            assert provider.external_ip("missing") == ""
            assert provider.internal_ip("missing") == ""


class TestCreateNode:
    def test_delegates_to_create_node_with_resources_and_labels(self):
        """``ray up``'s head-node bootstrap calls ``create_node`` directly
        (see ``ray.autoscaler._private.commands.get_or_create_head_node``),
        bypassing ``create_node_with_resources_and_labels``.
        """
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[])
        fake_client.workspaces.build_create_payload_from_names.side_effect = (
            lambda **kwargs: SimpleNamespace(payload={"name": kwargs["workspace_name"]})
        )
        fake_client.workspaces.create.side_effect = [
            {"id": "ws-1", "name": "node-1", "status": "pending"},
        ]
        tags = {
            TAG_RAY_CLUSTER_NAME: CLUSTER_NAME,
            TAG_RAY_USER_NODE_TYPE: "worker",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        }

        with _patched_from_env(fake_client):
            result = provider.create_node({}, tags, 1)

        assert result == {"ws-1": {"id": "ws-1", "name": "node-1", "status": "pending"}}

    def test_creates_head_node_without_a_cluster_name_tag(self):
        """Neither ``ray.autoscaler._private.commands.get_or_create_head_node``
        nor ``ray.autoscaler._private.node_launcher`` ever put
        ``TAG_RAY_CLUSTER_NAME`` into the tags passed to ``create_node``: every
        built-in provider treats the cluster name as always being
        ``self.cluster_name`` instead of expecting callers to supply it, and
        this provider must do the same.
        """
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[])
        fake_client.workspaces.build_create_payload_from_names.side_effect = (
            lambda **kwargs: SimpleNamespace(payload={"name": kwargs["workspace_name"]})
        )
        fake_client.workspaces.create.side_effect = [
            {"id": "ws-1", "name": "node-1", "status": "pending"},
        ]
        tags = {
            TAG_RAY_USER_NODE_TYPE: "head",
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        }

        with _patched_from_env(fake_client):
            result = provider.create_node({}, tags, 1)

        assert result == {"ws-1": {"id": "ws-1", "name": "node-1", "status": "pending"}}

    def test_waits_for_workspace_to_leave_creating_state(self):
        provider = _provider(workspace_creation_timeout=30)
        creating = {"id": "ws-1", "name": "node-1", "status": "creating"}
        running = {"id": "ws-1", "name": "node-1", "status": "running"}
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={})
        fake_client.workspaces.build_create_payload_from_names.side_effect = (
            lambda **kwargs: SimpleNamespace(payload={"name": kwargs["workspace_name"]})
        )
        fake_client.workspaces.create.return_value = creating
        fake_client.workspaces.get = AsyncMock(side_effect=[creating, running])
        tags = {
            TAG_RAY_USER_NODE_TYPE: "head",
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        }

        with (
            _patched_from_env(fake_client),
            patch("src_ray_provider.node_provider.asyncio.sleep", new_callable=AsyncMock) as sleep,
        ):
            result = provider.create_node({}, tags, 1)

        assert result == {"ws-1": running}
        assert fake_client.workspaces.get.await_count == 2
        assert sleep.await_count == 2

    def test_workspace_creation_wait_raises_after_configured_timeout(self):
        provider = _provider(workspace_creation_timeout=5)
        creating = {"id": "ws-1", "name": "node-1", "status": "creating"}
        clock = SimpleNamespace(now=0.0)
        fake_loop = SimpleNamespace(time=lambda: clock.now)
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={})
        fake_client.workspaces.build_create_payload_from_names.side_effect = (
            lambda **kwargs: SimpleNamespace(payload={"name": kwargs["workspace_name"]})
        )
        fake_client.workspaces.create.return_value = creating
        fake_client.workspaces.get = AsyncMock(return_value=creating)
        tags = {
            TAG_RAY_USER_NODE_TYPE: "head",
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        }

        async def advance_clock(seconds):
            clock.now += seconds

        with (
            _patched_from_env(fake_client),
            patch("src_ray_provider.node_provider.asyncio.get_running_loop", return_value=fake_loop),
            patch("src_ray_provider.node_provider.asyncio.sleep", side_effect=advance_clock),
            pytest.raises(TimeoutError, match="remained in 'creating' state for 5 seconds"),
        ):
            provider.create_node({}, tags, 1)

        assert fake_client.workspaces.get.await_count == 1

    def test_creates_requested_count_sequentially_in_one_client_session(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[])
        fake_client.workspaces.build_create_payload_from_names.side_effect = (
            lambda **kwargs: SimpleNamespace(payload={"name": kwargs["workspace_name"]})
        )
        fake_client.workspaces.create.side_effect = [
            {"id": "ws-1", "name": "node-1", "status": "pending"},
            {"id": "ws-2", "name": "node-2", "status": "pending"},
        ]
        tags = {
            TAG_RAY_CLUSTER_NAME: CLUSTER_NAME,
            TAG_RAY_USER_NODE_TYPE: "worker",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        }

        with _patched_from_env(fake_client) as from_env:
            result = provider.create_node_with_resources_and_labels(
                {}, tags, 2, {"CPU": 4}, {"purpose": "test"}
            )

        assert list(result) == ["ws-1", "ws-2"]
        assert [
            call.kwargs["workspace_name"]
            for call in fake_client.workspaces.build_create_payload_from_names.await_args_list
        ] == [
            call.args[0]["name"]
            for call in fake_client.workspaces.create.await_args_list
        ]
        assert all(
            call.kwargs["size_flavour_name"] == "4 Core - 16 GB" and call.kwargs["use_private_network"] is True
            for call in fake_client.workspaces.build_create_payload_from_names.await_args_list
        )
        from_env.assert_called_once_with()

    def test_continues_after_an_api_rejection_and_returns_partial_success(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[])
        fake_client.workspaces.build_create_payload_from_names.side_effect = (
            lambda **kwargs: SimpleNamespace(payload={"name": kwargs["workspace_name"]})
        )
        fake_client.workspaces.create.side_effect = [
            ApiError(400, "workspaces/", {"detail": "invalid"}),
            {"id": "ws-2", "name": "node-2", "status": "pending"},
        ]
        tags = {
            TAG_RAY_CLUSTER_NAME: CLUSTER_NAME,
            TAG_RAY_USER_NODE_TYPE: "worker",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        }

        with _patched_from_env(fake_client):
            result = provider.create_node_with_resources_and_labels({}, tags, 2, {}, {})

        assert result == {"ws-2": {"id": "ws-2", "name": "node-2", "status": "pending"}}
        assert fake_client.workspaces.create.await_count == 2

    @pytest.mark.parametrize("count", [-1, 1.5])
    def test_rejects_invalid_counts(self, count):
        provider = _provider()
        with pytest.raises(ValueError, match="non-negative integer"):
            provider.create_node_with_resources_and_labels({}, {}, count, {}, {})

    def test_rejects_missing_or_inconsistent_identity_tags(self):
        provider = _provider()
        with pytest.raises(ValueError, match=TAG_RAY_USER_NODE_TYPE):
            provider.create_node_with_resources_and_labels({}, {}, 1, {}, {})

        tags = {
            TAG_RAY_CLUSTER_NAME: CLUSTER_NAME,
            TAG_RAY_USER_NODE_TYPE: "worker",
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        }
        with pytest.raises(ValueError, match=TAG_RAY_NODE_KIND):
            provider.create_node_with_resources_and_labels({}, tags, 1, {}, {})


class TestNodeLifecycleFor404:
    def test_is_running_is_false_for_a_workspace_that_no_longer_exists(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={})

        with _patched_from_env(fake_client):
            assert provider.is_running("ws-missing") is False

    def test_is_terminated_is_true_for_a_workspace_that_no_longer_exists(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[], get_by_id={})

        with _patched_from_env(fake_client):
            assert provider.is_terminated("ws-missing") is True

    def test_non_404_api_errors_are_not_swallowed(self):
        provider = _provider()
        fake_client = _FakeClient(co={"id": "co-1"}, workspaces=[])
        fake_client.workspaces.get = AsyncMock(
            side_effect=ApiError(500, "workspaces/ws-1/", {"detail": "boom"})
        )

        with _patched_from_env(fake_client), pytest.raises(ApiError):
            provider.is_running("ws-1")
