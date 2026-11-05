# External State Join in {{product-name}} Flow (Java and Kotlin)

This pipeline enriches queue events from a prebuilt dynamic reference table. Java and Kotlin use the same read-only joined-state API.

[Java source]({{source-root}}/yt/yt/flow/examples/java/external_state_join)

[Kotlin source]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join)

## Pipeline structure {#structure}

`event_reader` publishes the `event` stream. The `lookup_join` companion computation is grouped by `key`, reads `/reference` through `external_state_joiners`, and sends successful matches to `enriched`. The queue sink stores that output. The joiner's `path` may point to a Cypress link, allowing the reference table to be switched without restarting the pipeline.

See the [Java pipeline spec]({{source-root}}/yt/yt/flow/examples/java/external_state_join/external_state_join/src/main/resources/pipeline.yson) or [Kotlin pipeline spec]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join/external_state_join/src/main/resources/pipeline.yson).

## Read the joined state {#lookup}

`StateDescriptors.externalReadOnly("/reference")` binds the process function to the joiner declared under the same name. The accessor selects the row by the current message key. A missing row returns no `name`, so the example emits nothing. The joined accessor does not provide a write path.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/external_state_join/external_state_join/src/main/java/tech/ytsaurus/flow/examples/externalstatejoin/LookupJoinFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/external_state_join/external_state_join/src/main/kotlin/tech/ytsaurus/flow/examples/externalstatejoin/LookupJoin.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' %}

{% endlist %}

The function is registered as `lookup_join` through `@FlowComputation`; the pipeline spec declares the `event` and `enriched` stream schemas.

## See also

- [External State (Java)](../../../../flow/java/external-state.md)
- [Static Table Join example](../../../../flow/java/examples/static_table_join.md)
