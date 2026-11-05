# Static Table Join in {{product-name}} Flow (Java and Kotlin)

This pipeline loads a static reference table into external state and enriches queue events by the shared key. Java and Kotlin use the same SDK and pipeline topology.

[Java source]({{source-root}}/yt/yt/flow/examples/java/static_table_join)

[Kotlin source]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join)

## Pipeline structure {#structure}

- `reference_reader` reads the static reference table;
- `reference_loader` normalizes each reference value and writes it to `/reference_state` through `external_state_managers`;
- `event_reader` reads events from a queue;
- `enricher` reads `/reference_state` through `external_state_joiners` and publishes only successful matches to `enriched`.

The manager and joiner point to the same dynamic state table. Their keys must match the `group_by_schema` of their computations. See the [Java pipeline spec]({{source-root}}/yt/yt/flow/examples/java/static_table_join/static_table_join/src/main/resources/pipeline.yson) or [Kotlin pipeline spec]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join/static_table_join/src/main/resources/pipeline.yson).

## Load the reference state {#load}

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/static_table_join/static_table_join/src/main/java/tech/ytsaurus/flow/examples/statictablejoin/ReferenceLoaderFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/static_table_join/static_table_join/src/main/kotlin/tech/ytsaurus/flow/examples/statictablejoin/ReferenceLoader.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' %}

{% endlist %}

## Enrich events {#enrich}

The read-only descriptor selects the joined row by the current message key. If the row or `normalized_name` is absent, the example produces no output.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/static_table_join/static_table_join/src/main/java/tech/ytsaurus/flow/examples/statictablejoin/EnricherFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/static_table_join/static_table_join/src/main/kotlin/tech/ytsaurus/flow/examples/statictablejoin/Enricher.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' %}

{% endlist %}

Both computations are discovered through `@FlowComputation`; the pipeline spec declares the three stream schemas used by the native sources and companion functions.

## See also

- [External State (Java)](../../../../flow/java/external-state.md)
- [External State Join example](../../../../flow/java/examples/external_state_join.md)
