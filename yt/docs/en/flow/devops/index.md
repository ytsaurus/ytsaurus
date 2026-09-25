# Operating {{product-name}} Flow

This section explains how to deploy a pipeline, inspect its state, and investigate stalled processing. The commands assume cluster access and permissions on the pipeline node. Replace `//path/to/pipeline`, `<cluster>`, and `<operation-id>` with your own values.

## Deployment {#deployment}

Follow [initial Vanilla deployment](vanilla/initial-deploy.md) to start the controller and workers as tasks of one operation. For Docker/CRI jobs, including a typical Kubernetes installation, see [Docker job environment and network setup](vanilla/docker-environment.md); the C++ example needs no custom image but may need cluster-name resolution and external proxy access. Flow creates and mounts its internal tables with the pipeline by default. If your pipeline uses user-managed external state, choose an [external state table type](state-tables-choice.md) before creating those tables based on availability and cost requirements.

## Launch Flow {#launch-flow}

Create the configuration and start the pipeline with the [Vanilla launch steps](vanilla/initial-deploy.md#run). Before starting, verify the objects and permissions required by the new version.

## Deployment guides {#deployment-pages}

To operate an existing pipeline, use the [lifecycle guide](vanilla/pipeline-operations.md), [releases](vanilla/releases.md), [security](vanilla/security.md), and [logs](vanilla/diagnostics/logs.md).

## Lifecycle {#lifecycle}

Use [`yt flow`](../tools/cli.md) to start, stop, pause, and inspect the pipeline. `stop-pipeline` drains intermediate buffers; `pause-pipeline` suspends processing without draining. [Complete removal](vanilla/pipeline-operations.md#remove) also aborts the Vanilla operation and deletes the pipeline node with its Flow internal tables. User-managed external state tables outside that node remain.

For upgrades, hotfixes, and rollbacks, follow the [release guide](vanilla/releases.md). Pass tokens and other secrets according to the [security guide](vanilla/security.md); keep them out of specs and images.

## When progress stops {#investigate}

Query the pipeline state and view, then compare them with the [controller and worker logs](vanilla/diagnostics/logs.md). [Diagnostics](diagnostics.md) helps identify the affected job, [profiling](profiling.md) helps locate a stall or bottleneck, and [troubleshooting](troubleshooting.md) maps observations to safe next actions.
