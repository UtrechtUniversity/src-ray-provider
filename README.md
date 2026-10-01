# SRC Ray Provider

Ray autoscaler node provider for SURF ResearchCloud (SRC). This project is
structured as a standalone Python package and can be copied into its own
`UtrechtUniversity/src-ray-provider` repository.

## Installation

Install the package in the environment that runs `ray up`:

```sh
python -m pip install .
```

The package depends on the ResearchCloud API SDK directly from
[`UtrechtUniversity/src-api-sdk`](https://github.com/UtrechtUniversity/src-api-sdk).
When the provider bootstraps the full cluster config, it adds a
`head_setup_commands` entry to install this package from
`UtrechtUniversity/src-ray-provider`. That runs on a newly provisioned head
before the Ray head process starts, making both the provider module and SDK
available to the autoscaler monitor.

The install source currently tracks the repository's default branch so this
project can be tested before its first release. Pin the install URL to a
release tag or commit before production use.

## Cluster config

See [`examples/cluster.yaml`](examples/cluster.yaml) for a complete sample.
The provider type is:

```yaml
provider:
  type: external
  module: src_ray_provider.node_provider.ResearchCloudNodeProvider
```

Ray's built-in provider types are a fixed set (`aws`, `gcp`, `local`, etc.); a
custom provider class must be loaded via the `external` type with `module` set
to the dotted class path, not as the `type` value directly.

The `provider` block also requires `co_name` and `wallet_name`. Ray's
`head_node_type` and `available_node_types` are used to derive SRC sizing.
Provide `auth.ssh_public_key` for the SRC catalog item's `ray_public_key`
interactive parameter. **`auth.ssh_private_key` must also be set** whenever
`ssh_public_key` is: Ray only copies the private key onto the head node (so
its autoscaler monitor can SSH into newly created worker nodes) when
`ssh_private_key` is explicitly configured. Without it, worker workspaces are
still created successfully but every SSH attempt into them fails with
"permission denied" -- so `bootstrap_config` rejects `ssh_public_key` without
a matching `ssh_private_key` up front. Both keys must be readable from the
machine running `ray up`.

If neither `auth.ssh_public_key` nor `auth.ssh_private_key` is set,
`bootstrap_config` generates its own ed25519 keypair instead -- nothing in
Ray generates SSH credentials for the `external` provider type on its own
(unlike e.g. its AWS or vSphere providers, which create a keypair in this
situation), so without this fallback no workspace would have any authorized
key and nothing could SSH into it. The generated keypair is cached under
`~/.cache/src_ray_provider/ssh_keys/<cluster_name>/` and reused across
repeated `ray up` invocations for the same `cluster_name` so the head node
keeps recognizing it; `auth.ssh_private_key`/`ssh_public_key` are written
back into the in-memory cluster config (not the on-disk cluster.yaml) so Ray
still syncs the private key onto the head node as usual.

When an SRC workspace is returned in the `creating` state, the provider polls
until that state changes before returning the node to Ray. The wait is bounded
by `provider.workspace_creation_timeout` (seconds), which defaults to 1800:

```yaml
provider:
  workspace_creation_timeout: 1800
```

The provider checks every five seconds and raises a timeout error if the
workspace remains `creating` past this limit.

To run the example, replace its SRC names, SSH settings, and test token. The
sample token setup is intentionally insecure and is only for disposable
testing. See [`MANUAL_TEST_PLAN.md`](MANUAL_TEST_PLAN.md).

## Development

```sh
python -m pip install -e '.[test]'
pytest
ruff check src tests
```

Unit tests mock the ResearchCloud client and do not call live endpoints.
