# Quick start with {{product-name}} Flow (YQL)

Use YQL over Flow to describe a [pipeline](../../../flow/concepts/glossary.md#pipeline) for streaming data processing as a declarative SQL query — without writing code in [C++](../../../flow/cpp/getting-started.md), [Java](../../../flow/java/getting-started.md), or [Python](../../../flow/python/getting-started.md). The pipeline runs as a vanilla operation on the selected {{product-name}} cluster.

{% note warning %}

This feature is under active development, and not all planned functionality is available yet.{% if audience == "internal" %} If you run into any issues, contact the Yandex Messenger chat [YT Flow Public](https://nda.ya.ru/t/MBW0Jgy-7bH78f).{% endif %}

{% endnote %}

{% if audience == "internal" %}
{% note info %}

If you need to run a YQL `SELECT` in a separate node of a regular pipeline instead of describing the whole pipeline with a query, see [YQL computation in {{product-name}} Flow](../../../yandex-specific/flow/extensions/yql.md).

{% endnote %}
{% endif %}

## Useful links

- [YQL documentation](../../../yql/index.md) — complete reference for YQL syntax
- [YQL provider for YT Flow]({{source-root}}/yt/yql/providers/ytflow) — source code
{% if audience == "internal" %}- Questions and feature requests: chat [YT Flow Public](https://nda.ya.ru/t/hcJkQdBD7LNa9V) or the [YQLOVERYT](https://st.yandex-team.ru/YQLOVERYT) queue{% endif %}

## Pragmas {#pragmas}

You control a YQL over Flow query with a set of pragmas:

| Pragma | Description |
|---|---|
| `PRAGMA Engine = "ytflow";` | Selects the Flow engine to run the query |
| `PRAGMA Ytflow.Cluster = "...";` | Cluster for the pipeline’s internal tables and output ordered queues |
| `PRAGMA Ytflow.RuntimeCluster = "...";` | Cluster to run the vanilla operation.{% if audience == "internal" %} Vanga is recommended as a cross-DC cluster with high availability{% endif %} |
| `PRAGMA Ytflow.PipelineDirectory = "...";` | Path to the directory with pipelines in {{product-name}} |
| `PRAGMA Ytflow.PipelineName = "...";` | Pipeline name. Full path: `{pipeline_directory}/{pipeline_name}` |
| `PRAGMA Ytflow.WorkerCount = "...";` | Number of worker jobs for the vanilla operation |
{% if audience == "internal" %}| `PRAGMA Ytflow.LogbrokerConsumerPath = "...";` | Path to the [Logbroker](../../../yandex-specific/flow/extensions/logbroker.md) consumer (only when reading from Logbroker) |
{% endif %}

## First query {#first-query}

Example: row-by-row transformation of a [stream](../../../flow/concepts/glossary.md#stream-and-computation) (map).

```yql
-- select the Flow engine
PRAGMA Engine = "ytflow";

-- cluster for the pipeline’s internal tables
PRAGMA Ytflow.Cluster = "{{flow-data-cluster}}";
-- cluster for the vanilla operation
PRAGMA Ytflow.RuntimeCluster = "{{flow-runtime-cluster}}";
-- directory with pipelines
PRAGMA Ytflow.PipelineDirectory = "//home/my-project/pipelines";
-- pipeline name
PRAGMA Ytflow.PipelineName = "my-pipeline";
-- number of workers
PRAGMA Ytflow.WorkerCount = "1";

-- read from the input queue, transform, write to the output queue
INSERT INTO
    {{flow-data-cluster}}.`//home/my-project/output_queues/sink_queue`
SELECT
    string_field || "_processed" AS string_field,
    int64_field,
    EndsWith(string_field, "bar") AS predicate
FROM
    {{flow-data-cluster}}.`//home/my-project/input_queues/source_queue`
WHERE int64_field > 1;
```

The query starts a pipeline that continuously processes messages from the input queue and writes the results to the output queue. The output table schemas are automatically inferred from the query.

For a description of all supported YQL constructs, see the [Supported constructs](../../../flow/yql/features.md) section.

## How to run {#how-to-run}

{% note info "Prerequisites" %}

You need read and write permissions for all directories mentioned in the query, as well as a compute quota on the {{product-name}} cluster specified as `Ytflow.RuntimeCluster`.

{% endnote %}

You can run the query in two ways:

**Via the {{product-name}} UI**: open the **Queries** tab on the runtime cluster and run the query.

**Via the Python client**:

```python
from yt.wrapper import YtClient

# any production cluster
client = YtClient('{{flow-data-cluster}}')

# run the query and wait for completion
client.run_query(
    engine='yql',
    settings=dict(
        # pass the runtime cluster here
        cluster='{{flow-runtime-cluster}}',
    ),
    query='<YQL query>',
    sync=True,
)
```

After the query finishes, a pipeline starts on the cluster and runs continuously. If a pipeline with the same name already exists, it stops and finishes processing all internal streams, then the new version starts.

## Monitoring {#monitoring}

To track the running pipeline, you can use:

{% if audience == "internal" %}- **Processing graph** with stream characteristics and resource consumption — the **Flow** tab for the pipeline in the {{product-name}} UI.{% endif %}
- **Dashboard** — **Flow → Monitoring** tab.
- **Controller logs** (worker status, possible issues):
  ```bash
  {{yt-cli}} --proxy <pipeline-cluster> flow show-logs //home/my-project/pipelines/my-pipeline
  ```
- **Job logs** — via the vanilla operation, which is available through the link from the `flowPublish` cube in the pipeline graph.

## See also

- [Supported constructs](../../../flow/yql/features.md)
- [Basic concepts](../../../flow/concepts/glossary.md)
- [Connectors](../../../flow/connectors/about.md)