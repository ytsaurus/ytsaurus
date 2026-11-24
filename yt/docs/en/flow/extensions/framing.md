{% include [Framing library](../../_includes/flow/extensions/framing.md) %}

## Connector integration {#connectors}

Shared helper parameters do not configure a connector. Connectors may use different batching rules, limits, defaults, and unpacking-error policies. Do not apply these settings to `PackBatch`, `PackBatches`, or `UnpackBatch`. See {% if audience == "public" %}[Working with YDB](ydb.md) and {% endif %}[Working with Kafka](kafka.md) for connector configuration.

## See also {#see-also}

- [Library source code]({{source-root}}/yt/yt/flow/extensions/framing)
{% if audience == "public" %}- [Working with YDB](ydb.md){% endif %}
- [Working with Kafka](kafka.md)
