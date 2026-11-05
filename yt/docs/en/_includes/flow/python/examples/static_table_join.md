# Static Table Join in {{product-name}} Flow (Python)

This pipeline loads a static reference table into external state and enriches queue events by the shared key.

[Source code]({{source-root}}/yt/yt/flow/examples/python/static_table_join)

## Pipeline structure {#structure}

- `reference_reader` reads the static table;
- `reference_loader` normalizes reference values and writes them to `/reference_state`;
- `event_reader` reads queue events;
- `enricher` performs a read-only lookup through the joiner named `/reference_state` and writes successful matches to `enriched`.

The manager and joiner point to the same dynamic state table. Their keys must match the computations' `group_by_schema`. See the [pipeline spec]({{source-root}}/yt/yt/flow/examples/python/static_table_join/pipeline.yson).

## Load and join {#implementation}

The loader uses `ctx.external_state(...)` and persists the normalized value. The enricher uses `ctx.joined_external_state(...)`; when `normalized_name` is absent, it emits nothing.

{% code '/yt/yt/flow/examples/python/static_table_join/__main__.py' lang='python' lines='[BEGIN reference_loader]-[END reference_loader]' %}

{% code '/yt/yt/flow/examples/python/static_table_join/__main__.py' lang='python' lines='[BEGIN enricher]-[END enricher]' %}

## See also

- [External State (Python)](../../../../flow/python/external-state.md)
- [External State Join example](../../../../flow/python/examples/external_state_join.md)
