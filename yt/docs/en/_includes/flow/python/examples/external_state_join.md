# External State Join in {{product-name}} Flow (Python)

This pipeline enriches queue events from a prebuilt dynamic reference table through a read-only external state joiner.

[Source code]({{source-root}}/yt/yt/flow/examples/python/external_state_join)

## Pipeline structure {#structure}

`event_reader` publishes the `event` stream. The `lookup_join` companion computation is grouped by `key`, reads `/reference` through `external_state_joiners`, and sends successful matches to `enriched`. The queue sink stores the output. The joiner's `path` may point to a Cypress link, allowing the reference table to be switched without restarting the pipeline.

See the [pipeline spec]({{source-root}}/yt/yt/flow/examples/python/external_state_join/pipeline.yson).

## Read the joined state {#lookup}

`ctx.joined_external_state("/reference", message)` selects the joined row by the message key. The returned accessor is read-only. A missing row is represented by an empty payload, so `get("name")` returns `None` and the example emits nothing.

{% code '/yt/yt/flow/examples/python/external_state_join/__main__.py' lang='python' lines='[BEGIN lookup_join]-[END lookup_join]' %}

## See also

- [External State (Python)](../../../../flow/python/external-state.md)
- [Static Table Join example](../../../../flow/python/examples/static_table_join.md)
