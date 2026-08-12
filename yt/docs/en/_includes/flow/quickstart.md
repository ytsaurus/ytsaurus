# Quick start with {{product-name}} Flow

Use this guide to get familiar with the Flow infrastructure by working through a minimal pipeline example. In this guide, you’ll:

- Locally run a pipeline in long-lived mode to explore it.
- Study the project structure.
- Run useful commands to work with the pipeline.

## Main pipeline components

A single pipeline’s architecture includes three main components:

- [Controller](../../flow/concepts/glossary.md#controller) — manages the pipeline’s lifecycle.
- [Worker](../../flow/concepts/glossary.md#worker) — reads from sources and performs computations.
- {{product-name}} Cluster — stores system tables in the pipeline directory.

Each pipeline needs its own set of Controller and Worker instances, a working directory in [Cypress](../../user-guide/storage/cypress.md), and a set of system dynamic tables.

## Prerequisites

To work with the pipeline, you need:

- A Linux x86_64 virtual machine to compile the YT Flow C++ project (we recommend at least 6 vCPU, 12 GB RAM, and 120 GB SSD).
- A local copy of the [repository]({{source-root}}) (in the examples below, `~/arcadia` is used as its path).
- The `ya` utility installed.
- A [YT token](../../user-guide/storage/auth.md) for the {{product-name}} cluster with dynamic tables{% if audience == "internal" %} (see the [list of clusters](../../user-guide/dynamic-tables/clusters.md)){% endif %}, for example, {{flow-example-cluster}}.

## Run the pipeline {#start}

Use the ready-made directory to run a minimal NoOp pipeline locally:

```bash
$ cd ~/arcadia/yt/yt/flow/examples/cpp/noop

# Build the project.
$ ya make

# The script starts the YT Flow components (Controller, Worker) and the pipeline.
# The script creates the directory //tmp/$(whoami)/pipelines/pipeline for YT Flow system objects.
$ ./run_noop_pipeline.sh --cluster {{flow-example-cluster}} --path //tmp/$(whoami)/pipelines
```

## Project structure {#structure}

Below is a minimal working example to help you get familiar with the setup.

{% if audience == "internal" %}

```bash
$ tree -L 2
.
├── README.md                  # Documentation for run_noop_pipeline.sh.
├── controller.config.yson     # Configuration file for the Controller.
├── worker.config.yson         # Configuration file for the Worker.
├── pipeline
│   ├── main.cpp               # Pipeline code: computation TNoopComputation and launch in `int main`.
│   ├── pipeline.yson          # Pipeline specification (computation graph topology).
│   └── ya.make
└── yt_sync
    ├── __main__.py            # Creates the structures in {{product-name}} that the pipeline needs to run.
    ├── pipelines.py           # Describes the "pipeline" working directory.
    ├── stages.py              # Describes the {{product-name}} cluster where the pipeline will run.
    └── ya.make
```

{% else %}

```bash
$ tree -L 2
.
├── README.md                  # Documentation for run_noop_pipeline.sh.
├── controller.config.yson     # Configuration file for the Controller.
├── worker.config.yson         # Configuration file for the Worker.
├── pipeline
│   ├── main.cpp               # Pipeline code: computation TNoopComputation and launch in `int main`.
│   ├── pipeline.yson          # Pipeline specification (computation graph topology).
│   └── ya.make
└── yt_sync_mini
    ├── __main__.py            # Creates the structures in {{product-name}} that the pipeline needs to run.
    └── ya.make
```

{% endif %}

What happens in the pipeline:

- `TRandomSource` generates random messages.
- `TNoopComputation` reads them and discards them.

Pipeline launch sequence (the code is slightly simplified compared to the script).

First, a Cypress object of type `pipeline` is created, along with the set of helper dynamic tables that Flow needs to run (see the [Pipeline Object](../../flow/concepts/pipeline-object.md) section).

{% if audience == "internal" %}

In the Yandex infrastructure, [YtSync](../../flow/concepts/pipeline-object.md#yt-sync) does this:

```bash
$ TEST_CLUSTER={{flow-example-cluster}} TEST_YT_PATH=//tmp/$(whoami)/pipelines \
    ./yt_sync/yt_sync --stage test --scenario ensure --parallel-factor 0 --commit
```

{% else %}

In the open-source version, you use the ready-made helper [yt_sync_mini](../../flow/concepts/pipeline-object.md#yt-sync-mini):

```bash
$ TEST_YT_CLUSTER={{flow-example-cluster}} TEST_YT_PATH=//tmp/$(whoami)/pipelines/pipeline \
    ./yt_sync_mini/yt_sync_mini
```

{% endif %}

Next, the long-lived Controller and Worker are started, and the pipeline specification is sent to the Controller:

```bash
# Start the long-lived Controller and Worker.
$ YT_FLOW_MODE=Controller pipeline/pipeline --config controller.config.yson
$ YT_FLOW_MODE=Worker pipeline/pipeline --config worker.config.yson

# Send the pipeline.yson specification to the Controller.
# Wait until the pipeline reaches the Working state.
$ YT_FLOW_WAIT=0 pipeline/pipeline --config pipeline.yson
```

You can find the structure of the [internal tables](../../flow/concepts/pipeline-object.md#internal_tables) that were created with the `pipeline` object here:

`{{yt-cli}} --proxy={{flow-example-cluster}} list //tmp/$(whoami)/pipelines/pipeline`

## Useful commands {#commands}

Check that the pipeline is running:

```bash
$ {{yt-cli}} --proxy {{flow-example-cluster}} flow get-pipeline-state --pipeline-path //tmp/$(whoami)/pipelines/pipeline
working
```

You can view detailed information and statistics about the pipeline in the following ways:

```bash
$ curl http://localhost:10002/orchid/job_tracker/jobs | {% if audience == "internal" %}ya tool {% endif %}jq
$ {{yt-cli}} --proxy {{flow-example-cluster}} flow describe-pipeline --pipeline-path //tmp/$(whoami)/pipelines/pipeline
```

You can view the Controller and Worker logs locally:

```bash
$ ls *.log
controller.log  worker.log
```

See [Logs](../../flow/release/logs.md) for how to read the controller and worker logs.

Pipeline graph visualization:

```bash
$ cd ~/arcadia/yt/yt/flow/tools/draw_pipeline_graph

$ ya run . -- --input {{flow-example-cluster}}://tmp/example/noop --ttl 1
```

This creates an .svg file with the pipeline image. You can open the file in a browser. For information about what you can learn from the graph, read the README.md file next to the utility.

![](../../flow/images/flow_noop_pipeline.png =600x230){ .center }

## Run in a YT vanilla operation {#vanilla}

If your pipeline uses `TSimpleRunnerProgram` (as in all `examples/cpp/*` examples), you can run it in a vanilla operation by adding the `vanilla` block to the config:

```yson
{
    "cluster_url" = "{{flow-example-cluster}}";
    "path" = "//tmp/example/pipeline";
    "spec" = { ... };
    "vanilla" = {
        "enable" = %true;
        "pool" = "research";
        "worker" = {"count" = 4};
    };
}
```

Required parameters: `worker.count`, `pool`. Other parameters have reasonable default values: the controller is 1 job × (1 CPU, 4 GB), the worker is 4 CPU and 4 GB per job, and the in-job ports are fixed (`rpc_port = 10080`, `monitoring_port = 10081`, `companion.port = 10082`). When you run the binary, it creates a vanilla operation with two tasks (controller + worker), submits the pipeline for execution, and waits for completion.

For the full list of fields, see [TVanillaConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) (also see [TVanillaTaskConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig)).

## What’s next

For a deeper dive into the framework, follow the instructions in the [Getting started](../../flow/start.md) section.