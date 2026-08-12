# Pipeline Object in {{product-name}}

This section describes the Cypress object `pipeline` — the deployment unit of {{product-name}} Flow. It covers the data model, creation methods, internal tables, and management tools.

Related sections: [Pipeline in the glossary](../../../flow/concepts/glossary.md#pipeline), [Spec, DynamicSpec and Config](../../../flow/concepts/spec.md), [Stateful processing](../../../flow/concepts/stateful.md), [Pipeline internal tables](../../../flow/concepts/glossary.md#inner-pipeline-tables).

## Data Model { #data_model }

In {{product-name}}, a *pipeline* is a special Cypress object of type `pipeline`. Structurally, it’s a map node — a directory where a set of service [internal tables](#internal_tables) required for Flow operation are automatically created.

## Internal Tables { #internal_tables }

<small>Table 1 — Pipeline internal tables</small>

| Table name | Purpose |
| --- | --- |
| `input_messages` | Index of input messages for [transform computations](../../../flow/concepts/computation.md#ttransformcomputation), used for deduplication. |
| `compact_input_messages` | Compact index of input messages. It’s used by default for all computations, except when `experimental_enable_non_uint_key` is enabled; the behavior is overridden by the `use_compact_input_messages` parameter in [TComputationSpec](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TComputationSpec). |
| `compact_output_messages` | Not used. |
| `compact_partition_output_messages` | Output messages of transform computations, physically grouped by partitions and chunked by `stream_id` / `chunk_id` for optimal reading. |
| `states` | User and service [states](../../../flow/concepts/stateful.md) stored by [key](../../../flow/concepts/glossary.md#key). |
| `partition_states` | States stored by partition. |
| `timers` | [Timers](../../../flow/concepts/glossary.md#timer) of the user code. |
| `controller_logs` | Event logs of the [Controller](../../../flow/concepts/glossary.md#controller) in the PublicFlowController category. |
| `flow_state` | Current Flow state. |
| `flow_state_obsolete` | Flow KV storage for named objects (spec, dynamic_spec, etc.). |
| `partition_transactions` | Service table for safe retry of transactions. |

After you create a pipeline, these tables appear under the path `<pipeline_path>/<table_name>` and are automatically mounted.

{% note warning "Attention" %}

The internal tables are service tables, and their structure may change between Flow releases. Don’t read from or write to them directly from your user code. To read debugging data (for example, controller logs), use `yt flow show-logs` and other commands from the `yt flow` family.

{% endnote %}

## Creating a Pipeline { #create }

{% if audience == "internal" %}

### Via YtSync { #yt-sync }

In the Yandex infrastructure, the only supported way to create a pipeline is [YtSync]({{yt-sync-docs}}/). It provides a declarative description, correct physical attributes, schema migrations, and uniform management of related entities (user tables, queues, consumers).

You just need to describe the pipeline in `pipelines.py` using the `builtin:pipeline_preset` preset, and run the `ensure` scenario:

```bash
./yt_sync/yt_sync --scenario ensure --stage <stage> --commit
```

YtSync creates the pipeline and related entities (tables, queues, consumers) in a single declarative description.

{% else %}

### Via yt_sync_mini { #yt-sync-mini }

The recommended way to create a pipeline in open source is the Python library [`yt_sync_mini`]({{source-root}}/yt/yt/flow/library/python/yt_sync_mini). It creates a map node of type `pipeline`, all [internal tables](#internal_tables) with correct schemas and physical attributes, and mounts them immediately. The operation is idempotent — running it again on an existing pipeline is a no-op.

```python
import yt.wrapper as yt

from yt.yt.flow.library.python.yt_sync_mini import yt_sync_mini

client = yt.YtClient(proxy="<cluster>")
yt_sync_mini(client, "<pipeline_path>")
```

### Low-level Creation of a Cypress Node { #low-level-create }

If you need full control over node and table creation (for example, to integrate into an existing deployment system), you create the pipeline using the standard `create` mechanism — the same way as for other Cypress object types (table, map_node, queue_consumer, etc.). With this approach, you’re responsible for creating and mounting the internal tables with correct schemas and attributes.

#### Via {{product-name}} CLI

```bash
yt --proxy <cluster> create pipeline <pipeline_path>
```

#### Via Python ({{product-name}} wrapper)

```python
import yt.wrapper as yt

client = yt.YtClient(proxy="<cluster>")
client.create(
    "pipeline",
    "<pipeline_path>"
)
```

#### Via C++ ({{product-name}} native client)

```cpp
#include <yt/yt/flow/lib/native_client/pipeline_init.h>

NYT::NApi::TCreateNodeOptions options;

auto nodeId = NYT::NFlow::CreatePipelineNode(client, pipelinePath, options);
```

{% endif %}

## External State { #external-state }

When the schema of the [internal tables](#internal_tables) changes in a new Flow version, the format upgrade is performed via a separate migration — see [Pipeline internal tables](../../../flow/concepts/glossary.md#inner-pipeline-tables) and [Basic rollout rules](../../../flow/release/basic-rules.md).

If the pipeline uses [External State](../../../flow/concepts/stateful.md) (user tables outside the node), creating them and evolving their schemas is your responsibility. {% if audience == "internal" %}In the Yandex infrastructure, [YtSync]({{yt-sync-docs}}/) is used for this.{% else %}You perform the operations using standard commands like `yt create table ... --attributes '{dynamic=true; schema=...}'` and `yt mount-table` — see examples in the [Create command](../../../user-guide/storage/cypress-example.md#create) section.{% endif %}

## See also { #see_also }

- [Glossary: Pipeline](../../../flow/concepts/glossary.md#pipeline)
- [Pipeline internal tables](../../../flow/concepts/glossary.md#inner-pipeline-tables)
- [Basic pipeline rollout rules](../../../flow/release/basic-rules.md)
{% if audience != "internal" %}
- Open-source bootstrap script example: [`yt/yt/flow/examples/cpp/noop/yt_sync_mini`]({{source-root}}/yt/yt/flow/examples/cpp/noop/yt_sync_mini)
{% endif %}