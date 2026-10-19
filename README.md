# SRC Ray Provider

Ray autoscaler node provider for SURF ResearchCloud (SRC) (read more [here](https://utrechtuniversity.github.io/re-presentation-src-ray/slides.html#/ray-on-surf-researchcloud)). The autoscaler is meant to work in conjunction with the [SRC Ray component](https://github.com/UtrechtUniversity/src-component-ray).

**Currently in beta -- contact research.engineering@uu.nl if you would like to use this.** We will need to provide you with access to certain ResearchCloud catalog items.

## Short instructions

### Prerequisites

- A ResearchCloud CO with the "Ray Head Node" and "Ray Worker" Catalog Items available.
- In this CO, create a CO secret called `RAY_SRC_TOKEN` and fill it with an SRC API key.

**Security note**: to use Ray on SRC, you currently need to save an API key in a secret `RAY_SCR_TOKEN` inside your Collaboration. At the moment, ResearchCloud API keys cannot yet be scoped to a specific CO (coming soon), so it is best to test this package in a Collaboration to which only you have access.

### Steps

1. Clone this repo, `cd` into it.
1. Ensure you have a virtual environment active for this project.
1. [Install](#installation): `python -m pip install .`
1. [Edit cluster config](#cluster-config) (create `cluster.yaml`)
1. Set your ResearchCloud API token: `export RESEARCH_CLOUD_TOKEN="foo"`
1. `ray up cluster.yaml` to create the cluster. Wait for the head node to start.
1. `ray dashboard cluster.yaml` (to create an SSH tunnel to the head node, allowing you to submit jobs)
1. Submit a job from the example `jobs` directory: `ray job submit --address http://localhost:8265 --runtime-env-json='{"working_dir": "jobs", "pip": "jobs/requirements.txt"}' -- python3 test.py`

## Installation

Install this package in the environment that runs `ray up`:

```sh
python -m pip install .
```

The package depends on the ResearchCloud API SDK directly from
[`UtrechtUniversity/src-api-sdk`](https://github.com/UtrechtUniversity/src-api-sdk).

## Cluster config

See [`examples/cluster.yaml`](examples/cluster.yaml) for a complete sample.

The provider type is:

```yaml
provider:
  type: external
  module: src_ray_provider.node_provider.ResearchCloudNodeProvider
```

The `provider` block also requires `co_name` and `wallet_name`. Ray's
`head_node_type` and `available_node_types` are used to derive SRC sizing.

**Note:** if you set `auth.ssh_private_key` and `auth.ssh_public_key` in the
provider block to paths to those respective keys, **both keys will be uploaded to the head node**.
You may not want to do this with your normal private key. **Just
leave `auth.ssh_private_key` and `auth.ssh_public_key` unset, and the provider will generate a dedicated keypair instead.**

When an SRC workspace is returned in the `creating` state, the provider polls
until that state changes before returning the node to Ray. The wait is bounded
by `provider.workspace_creation_timeout` (seconds), which defaults to 1800:

```yaml
provider:
  workspace_creation_timeout: 1800
```

The provider checks every five seconds and raises a timeout error if the
workspace remains `creating` past this limit.

## Development

```sh
python -m pip install -e '.[test]'
pytest
ruff check src tests
```

Unit tests mock the ResearchCloud client and do not call live endpoints.
