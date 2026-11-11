from __future__ import annotations

import pytest

import src_ray_provider.node_provider as node_provider_module


@pytest.fixture(autouse=True)
def _isolated_cache_dirs(tmp_path, monkeypatch):
    """Keep generated keys and cached node tags out of the user's home directory.

    ``ResearchCloudNodeProvider`` persists mutable Ray tags (notably
    ``TAG_RAY_LAUNCH_CONFIG``) to ``NODE_TAG_CACHE_DIR`` since SRC
    workspaces have nowhere to store them (see ``_NodeTagCache`` in
    node_provider.py). Without this fixture, tests would read/write the
    real ``~/.cache/src_ray_provider/node_tags/`` on the machine running
    them. Bootstrap tests also generate and reuse SSH keypairs.
    """
    monkeypatch.setattr(node_provider_module, "NODE_TAG_CACHE_DIR", tmp_path / "node_tags")
    monkeypatch.setattr(node_provider_module, "SSH_KEY_CACHE_DIR", tmp_path / "ssh_keys")
