---
title: "YQL over Flow pragma reference"
description: "Settings for controlling YQL queries through Flow"
---

# YQL over {{product-name}} Flow pragma reference

## Syntax

[YQL over Flow](../../../flow/yql/features.md) pragma names start with `Ytflow`.<br>
An exception is `Engine`, common pragma for selecting query execution engine:

```yql
PRAGMA Ytflow.<pragma_name> = '<value>';
PRAGMA Engine = 'ytflow';
```

{% note warning %}

Pass pragma values as quoted strings. Use single `'...'` or double `"..."` quotes.

{% endnote %}

## Engine {#engine}

| Possible values | Default value | Type |
| --- | --- | --- |
| `'ytflow'` | — | Static |

Selects Flow for query execution.<br>
You must specify this pragma to run query through Flow.

```yql
PRAGMA Engine = 'ytflow';
```

## Ytflow.Cluster {#cluster}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

{{product-name}} cluster name for internal pipeline tables.

```yql
PRAGMA Ytflow.Cluster = '{{flow-data-cluster}}';
```

## Ytflow.RuntimeCluster {#runtime-cluster}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | Value of `Ytflow.Cluster` | Static |

Cluster name for running pipeline's [vanilla operation](../../../user-guide/data-processing/operations/vanilla.md).

Controllers and workers run as two task types (`controller` and `worker`) within a single operation; job counts and resources are configured separately for each task type.

```yql
PRAGMA Ytflow.RuntimeCluster = '{{flow-runtime-cluster}}';
```

## Ytflow.PipelineDirectory {#pipeline-directory}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path | — | Static |

Directory containing pipelines in Cypress on `Ytflow.Cluster`, used together with `Ytflow.PipelineName`.

Alternatively, set `Ytflow.PipelinePath` alone.

Example Cypress paths start with `//` and have no trailing `/`.

```yql
PRAGMA Ytflow.PipelineDirectory = '//home/my-project/pipelines';
```

## Ytflow.PipelineName {#pipeline-name}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

Pipeline name, used to create a subdirectory within `Ytflow.PipelineDirectory`,
where internal pipeline tables are stored.

Alternatively, explicitly set `Ytflow.PipelinePath`.

```yql
PRAGMA Ytflow.PipelineName = 'my-pipeline';
```

## Ytflow.PipelinePath {#pipeline-path}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path | `{Ytflow.PipelineDirectory}/{Ytflow.PipelineName}` | Static |

Full pipeline path in Cypress on `Ytflow.Cluster`;
this directory stores internal pipeline tables.

Alternatively, set `Ytflow.PipelineDirectory` and `Ytflow.PipelineName` separately.

If `Ytflow.PipelinePath` is specified, it takes precedence over those two pragmas.

```yql
PRAGMA Ytflow.PipelinePath = '//home/my-project/pipelines/my-pipeline';
```

## Ytflow.Auth {#auth}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | `default_ytflow` secret, if supplied by execution environment | Static |

Name of a YQL secret in category `ytflow` containing a token for authentication in {{product-name}}.

This token must grant access to pipeline tables on `Ytflow.Cluster`
and permission to start vanilla operation on `Ytflow.RuntimeCluster`.

For more information about authentication, see [Authentication](../../../flow/yql/authentication.md).

```yql
PRAGMA Ytflow.Auth = 'my_ytflow_credentials';
```

## Ytflow.WorkerCount {#worker-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | {% if audience == "internal" %}5{% else %}1{% endif %} | Static |

Number of worker jobs in vanilla operation on `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.WorkerCount = '5';
```

## Ytflow.WorkerCpuLimit {#worker-cpu-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number | {% if audience == "internal" %}10.0{% else %}1.0{% endif %} | Static |

Guaranteed CPU allocation per worker job in vanilla operation on `Ytflow.RuntimeCluster`.

Actual CPU consumption may be higher if spare resources are available on a job's host.

Unit: one CPU core.

```yql
PRAGMA Ytflow.WorkerCpuLimit = '10.0';
```

## Ytflow.WorkerMemoryLimit {#worker-memory-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Size (for example, `20G`) | {% if audience == "internal" %}20G{% else %}1G{% endif %} | Static |

Memory limit per worker job in vanilla operation on `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.WorkerMemoryLimit = '20G';
```

## Ytflow.EnableComputationPatternResources {#enable-computation-pattern-resources}

| Possible values | Default value | Type |
| --- | --- | --- |
| `true` / `false` | `false` | Static |

Enables reuse of computation patterns across graphs in one worker.

```yql
PRAGMA Ytflow.EnableComputationPatternResources = 'true';
```

## Ytflow.ControllerCount {#controller-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1 | Static |

Number of controller jobs in vanilla operation on `Ytflow.RuntimeCluster`.

One [controller](../../../flow/concepts/glossary.md#controller) becomes leader; others remain on standby.<br>
With a value of `1`, there is no standby controller, and pipeline management is temporarily unavailable while this sole controller restarts.

```yql
PRAGMA Ytflow.ControllerCount = '3';
```

## Ytflow.ControllerCpuLimit {#controller-cpu-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Floating-point number | {% if audience == "internal" %}3.0{% else %}1.0{% endif %} | Static |

Guaranteed CPU allocation per controller job in vanilla operation on `Ytflow.RuntimeCluster`.

Actual CPU consumption may be higher if spare resources are available on job's host.

Unit: one CPU core.

```yql
PRAGMA Ytflow.ControllerCpuLimit = '3.0';
```

## Ytflow.ControllerMemoryLimit {#controller-memory-limit}

| Possible values | Default value | Type |
| --- | --- | --- |
| Size (for example, `5G`) | {% if audience == "internal" %}5G{% else %}1G{% endif %} | Static |

Memory limit per controller job in vanilla operation on `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.ControllerMemoryLimit = '5G';
```

## Ytflow.GracefulUpdate {#graceful-update}

| Possible values | Default value | Type |
| --- | --- | --- |
| `true` / `false` | `true` | Static |

Pipeline update mode:
  * `true` — gracefully stop and finish processing internal streams ([`stop`](../../../flow/concepts/glossary.md#start-stop-pause-pipeline))
  * `false` — pause immediately ([`pause`](../../../flow/concepts/glossary.md#start-stop-pause-pipeline)).

Once pipeline reaches its target state, its previous vanilla operation on `Ytflow.RuntimeCluster` is aborted and new version starts.<br>
For restrictions on updating without draining, see [Hotfix](../../../flow/devops/vanilla/releases.md#hotfix); for stop commands, see [CLI reference](../../../flow/tools/cli.md).

```yql
PRAGMA Ytflow.GracefulUpdate = 'true';
```

## Ytflow.UpdateTimeout {#update-timeout}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval (`1d`, `5s`, `2w`) | 600s | Static |

Timeout for waiting for previous pipeline version to stop or pause during an update.

If pipeline does not reach its target state within this time, query execution fails. This update does not stop its previous vanilla operation on `Ytflow.RuntimeCluster` or start new version.<br>
Any stop or pause command already sent is not canceled.

```yql
PRAGMA Ytflow.UpdateTimeout = '600s';
```

## Ytflow.TabletCellBundle {#tablet-cell-bundle}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

[Tablet cell bundle](../../../user-guide/dynamic-tables/concepts.md#tablet_cell_bundles) (`tablet_cell_bundle`) for newly created tables.

```yql
PRAGMA Ytflow.TabletCellBundle = 'my-bundle';
```

## Ytflow.Account {#account}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

[Account](../../../user-guide/storage/accounts.md) for newly created tables.

```yql
PRAGMA Ytflow.Account = 'my-account';
```

## Ytflow.PrimaryMedium {#primary-medium}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

[Primary medium](../../../user-guide/storage/media.md) (`primary_medium`) for newly created tables.

```yql
PRAGMA Ytflow.PrimaryMedium = 'default';
```

## Ytflow.Pool {#pool}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

[Compute pool](../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) for pipeline's vanilla operation on `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.Pool = 'my-pool';
```

## Ytflow.YtPartitionCount {#yt-partition-count}

| Possible values | Default value | Type |
| --- | --- | --- |
| Positive number | 1 | Static |

Number of partitions in newly created output queues in {{product-name}}.

```yql
PRAGMA Ytflow.YtPartitionCount = '4';
```

## Ytflow.YtTtl {#yt-ttl}

| Possible values | Default value | Type |
| --- | --- | --- |
| Time interval (`1d`, `5s`, `2w`) | — | Static |

Retention period for rows in newly created output ordered dynamic tables (queues) in {{product-name}}.

Sets `max_data_ttl` and also sets:
  * `min_data_versions = 0`
  * `max_data_versions = 1`
  * `min_data_ttl = 0`
  * `auto_compaction_period` to half its TTL

For more information, see [Automatic removal of old rows](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md#remove_old_data).

```yql
PRAGMA Ytflow.YtTtl = '7d';
```

## Ytflow.YtConsumerVital {#yt-consumer-vital}

| Possible values | Default value | Type |
| --- | --- | --- |
| `true` / `false` | `false` | Static |

`vital` flag used when registering consumer with input queues in {{product-name}}.

When [automatic consumer-based queue trimming](../../../user-guide/dynamic-tables/queues.md#automatic_trimming) is enabled, queues respect vital consumer offsets and do not delete rows these consumers have not read yet.<br>
Non-vital consumer offsets do not prevent deletion of rows these consumers have not processed.

```yql
PRAGMA Ytflow.YtConsumerVital = 'true';
```

## Ytflow.YtConsumerDirectory {#yt-consumer-directory}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path | `{Ytflow.PipelinePath}/yql_ytflow/consumers` | Static |

{{product-name}} consumer directory.

```yql
PRAGMA Ytflow.YtConsumerDirectory = '//home/my-project/consumers';
```

## Ytflow.YtConsumerName {#yt-consumer-name}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | `default_consumer` | Static |

{{product-name}} consumer name.

```yql
PRAGMA Ytflow.YtConsumerName = 'my-consumer';
```

## Ytflow.YtProducerDirectory {#yt-producer-directory}

| Possible values | Default value | Type |
| --- | --- | --- |
| Path | `{Ytflow.PipelinePath}/yql_ytflow/producers` | Static |

{{product-name}} producer directory.

```yql
PRAGMA Ytflow.YtProducerDirectory = '//home/my-project/producers';
```

## Ytflow.YtProducerName {#yt-producer-name}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | `default_producer` | Static |

{{product-name}} producer name.

```yql
PRAGMA Ytflow.YtProducerName = 'my-producer';
```

{% if audience == "internal" %}
## Ytflow.LogbrokerConsumerPath {#logbroker-consumer-path}

| Possible values | Default value | Type |
| --- | --- | --- |
| String | — | Static |

Consumer path in specified Logbroker installation (Logbroker path, not a path in {{product-name}} Cypress).

For more information about Logbroker entities, see [Logbroker resource model](https://logbroker.yandex-team.ru/docs/concepts/resource_model).

```yql
PRAGMA Ytflow.LogbrokerConsumerPath = 'my-account/my-directory/my-consumer';
```
{% endif %}
