from __future__ import annotations

import pytest

import src_ray_provider.node_provider as node_provider_module


@pytest.fixture(autouse=True)
def _isolated_node_tag_cache_dir(tmp_path, monkeypatch):
    """Redirect the local node-tag cache to a temp dir for every test.

    ``ResearchCloudNodeProvider`` persists mutable Ray tags (notably
    ``TAG_RAY_LAUNCH_CONFIG``) to ``NODE_TAG_CACHE_DIR`` since SRC
    workspaces have nowhere to store them (see ``_NodeTagCache`` in
    node_provider.py). Without this fixture, tests would read/write the
    real ``~/.cache/src_ray_provider/node_tags/`` on the machine running
    them.
    """
    monkeypatch.setattr(node_provider_module, "NODE_TAG_CACHE_DIR", tmp_path / "node_tags")
