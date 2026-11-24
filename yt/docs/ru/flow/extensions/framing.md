{% include [Библиотека framing](../../_includes/flow/extensions/framing.md) %}

## Связь с коннекторами {#connectors}

Параметры общих функций не задают конфигурацию коннектора. Коннектор может использовать другую схему батчинга, собственные лимиты и значения по умолчанию, а также отдельно определять реакцию на ошибку распаковки. Не переносите эти настройки на `PackBatch`, `PackBatches` или `UnpackBatch`. Параметры коннекторов приведены на страницах {% if audience == "public" %}[Работа с YDB](ydb.md) и {% endif %}[Работа с Kafka](kafka.md).

## См. также {#see-also}

- [Исходный код библиотеки]({{source-root}}/yt/yt/flow/extensions/framing)
{% if audience == "public" %}- [Работа с YDB](ydb.md){% endif %}
- [Работа с Kafka](kafka.md)
