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
interactive parameter.

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
