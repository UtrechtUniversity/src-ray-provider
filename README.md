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
`head_node_type` and `available_node_types` are generated from the size
flavours available for the `Ray Head Node` and `Ray Worker` catalog items on
the configured cloud. Only CPU-only head
flavours are generated; the flavour with the smallest known CPU count becomes
`head` and is selected by default. If no usable CPU-only head flavour is
available, discovery fails. Worker flavours may include GPUs, and each
generated worker node type uses the top-level `max_workers` limit.
This catalog lookup happens before Ray validates the configuration during
`ray up`, so `RESEARCH_CLOUD_TOKEN` must be available when bootstrapping the
cluster. If Ray has cached an earlier configuration, use
`ray up --no-config-cache cluster.yaml` to refresh it.

You can override or extend the generated Ray configuration in cluster.yaml.
Values you provide are merged over the generated defaults. For example, set
`head_node_type` to another generated head type, tune worker limits/resources,
or set a custom `node_config.catalog_item_name` to use a different catalog
item:

```yaml
head_node_type: head_2_core_8_gb
available_node_types:
  worker_4_core_16_gb:
    max_workers: 2
  custom_worker:
    min_workers: 0
    max_workers: 1
    resources: {"CPU": 8}
    node_config:
      catalog_item_name: My Custom Ray Worker
      size_flavour_name: 8 Core - 32 GB RAM
```

To customize the default `head` through `available_node_types.head`, also set
`head_node_type: head` so Ray applies head-specific defaults before discovery.
Other generated node type names use a lowercase `head_` or `worker_` prefix
followed by the normalized size-flavour name. `node_config.catalog_item_name`
can be overridden per node type; the default remains `Ray Head Node` or `Ray Worker` according to the
generated type.

### Ray packages

The Ray catalog items' `packages` interactive parameter specifies
comma-separated Ray extras, as in `ray[client,tune]`. To override it for one
head or worker type, configure:

```yaml
head_node_type: head
available_node_types:
  head:
    node_config:
      ray_packages: "default,client"
  worker_4_core_16_gb:
    node_config:
      ray_packages: "client,tune"
```

The head defaults to `default,client,train,tune,serve`; it never infers its
packages from the machine running `ray up`. When the worker setting is omitted,
the provider infers worker extras from the Ray installation in
the Python environment running the provider. For autoscaling this is the head
node's environment, not the environment used to run `ray up`. An extra is
included when all of its applicable declared dependencies are installed at
compatible versions, including dependencies of nested extras. Extras with no
applicable dependencies are not inferred.

Python does not record which extras were requested, so this is an approximation:
dependencies installed for other reasons can cause an extra to be inferred,
and partially installed extras are omitted. Platform-specific dependencies
are checked against the head's platform. Extras do not capture arbitrary
packages or their exact versions, so use Ray runtime environments or setup
commands for those. Configure `ray_packages` explicitly when inference does
not reflect the desired worker installation. If no extras can be inferred,
worker creation fails with an instruction to configure them: the SRC component
interprets an empty parameter as its own defaults, not as bare `ray`.

The effective packages are part of paused-workspace reuse compatibility. If
they change, a head or worker created with different packages is not resumed.

Some GPU flavour names, such as `A10 - 1 GPU`, do not specify a CPU count.
These node types are generated with their GPU count but no CPU count; the
provider logs a warning unless you supply one. Set `resources.CPU` to the
actual number of CPUs for each such flavour:

```yaml
available_node_types:
  worker_a10_1_gpu:
    resources:
      CPU: 8  # Replace with the actual CPU count for this flavour.
```

Without this setting, Ray cannot scale from zero for tasks that require both
CPU and GPU resources. Once a worker is running, the autoscaler can learn its
runtime resources. Flavours with neither a known CPU nor GPU count are
skipped; define a custom node type with explicit resources if needed.

Paused and pausing workspaces are excluded from Ray's active node discovery,
whether or not reuse is enabled. With reuse disabled, if a replacement is
needed, a new workspace is created instead of resuming a manually paused
worker. Such a paused workspace is also excluded from ordinary `ray down`
discovery and must be deleted manually if no longer needed. If manually
resumed, it is discovered again on refresh and may be subject to Ray's normal
scaling decisions; manually resuming does not guarantee that the Ray worker
process is running.

### Reusing paused workspaces

Opt in to pause and reuse workspaces instead of deleting them:

```yaml
provider:
  resume_paused_workspaces: true
  resume_min_ttl_seconds: 86400  # default: 24 hours
```

Ray scale-down and `ray down` will **pause**, not delete, these workspaces.
They retain their data and may continue to incur costs until they expire or
are deleted manually. The provider waits for each pause to finish. Both head
and worker workspaces can be resumed on a later `ray up` or scale-up. A
candidate must belong to this cluster, match the requested catalog item and
size flavour and launch configuration, and have strictly more than the
configured number of seconds remaining before its `end_time`. Workspaces with
missing, invalid, or timezone-less expiry times are not reused.

Only workspaces previously created with reuse enabled are eligible: their
creation settings must match, including the SSH public key, Ray and Python
versions, and, for workers, the current head's private IP. Workspaces created
without this option are not assumed to have the setup required for reuse.
An incompatible or near-expiry workspace remains paused and a new one is created instead.
If `head_workspace_id` is configured, an incompatible paused head causes an error
rather than creating a different head.

On the same machine and account, workspace selection, submission, and pausing
are serialized. Across machines, avoiding duplicate claims of a paused
workspace relies on SRC atomically rejecting concurrent resume actions; a
rejected claim is reported as an error. The SDK does not expose a distributed
lock for new workspace creation, so do not run concurrent `ray up` commands
for the same cluster from different machines.

### Using an existing head workspace

When starting Ray manually on an existing ResearchCloud head workspace, set
`provider.head_workspace_id` to its ResearchCloud workspace ID (not its name,
hostname, Ray node ID, or IP address):

```yaml
cluster_name: my-unique-cluster
provider:
  type: external
  module: src_ray_provider.node_provider.ResearchCloudNodeProvider
  co_name: Example CO
  wallet_name: Example Wallet
  head_workspace_id: "<ResearchCloud workspace ID>"
head_node_type: head
available_node_types:
  head:
    min_workers: 0
    max_workers: 0
```

The workspace can have any name, but must be a non-terminal Compute workspace
visible in the configured CO. Its catalog item and size flavour must match
the selected head type in `provider.node_types`; choose the matching
`head_node_type` when the default head flavour is different. Its private IP
must match the address used to start the Ray head. A missing or mismatched
head, or another active workspace in this cluster resolving to the head type,
causes discovery to fail rather than launching a replacement. Without
`head_workspace_id`, existing name-based discovery remains unchanged.
Set the ID before starting the autoscaler; restart it when changing this
provider setting because the provider instance is not recreated on config reload.

Workers still use the `ray-<sanitized cluster_name>-` workspace name prefix.
Choose a unique, stable `cluster_name` for each cluster, since the provider
manages and can delete workspaces matching that prefix. Setting the head ID
does not protect the head from explicit termination (for example, `ray down`).
Keep the configured head resources consistent with the resources advertised by
`ray start`, and ensure the bootstrapped SSH key and config are accessible to
the head's autoscaler. Unlike `ray up`, manually starting the head does not
copy these files.

Set `auth.ssh_private_key` and `auth.ssh_public_key` together to paths to a
private key and its matching public key, or leave both unset to generate and
reuse a dedicated keypair for the cluster. Ray copies the private key to the
head node, so avoid using your normal private key. The provider reads the
public-key file during bootstrap and passes its contents to the SRC catalog
item.

The provider polls an SRC workspace until it reaches `running` before returning
the node to Ray. When reuse is enabled it also waits for `paused` on termination.
Each wait is bounded by `provider.workspace_creation_timeout`
(seconds), which defaults to 2400:

```yaml
provider:
  workspace_creation_timeout: 2400
```

The provider checks every five seconds and raises a timeout error if the
workspace does not reach `running` within this limit. Launch tags are cached
as soon as SRC returns the workspace ID, before the wait. Discovery and
workspace submission are coordinated on the same machine/account so Ray
does not see a newly submitted workspace before its launch tags are saved.
The cache and coordination are local: Ray commands on different machines or
accounts do not share the tags.

## Development

```sh
python -m pip install -e '.[test]'
pytest
ruff check src tests
```

Unit tests mock the ResearchCloud client and do not call live endpoints.
