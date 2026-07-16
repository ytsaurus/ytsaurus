# Поддержанные конструкции YQL в {{product-name}} Flow

{% note info %}

Данная страница описывает конструкции YQL, специфичные для работы с Flow. Общий синтаксис YQL описан в [документации YQL](../../../yql/index.md).

{% endnote %}

## Построчное преобразование потока (мап) {#map}

Основная конструкция — `INSERT INTO ... SELECT ... FROM`. Читает [стрим](../../../flow/concepts/glossary.md#stream-and-computation) из упорядоченной динамической таблицы (очереди {{product-name}}), применяет преобразование и записывает результат в другую очередь.

Пример части запроса:

```yql
INSERT INTO
    {{flow-data-cluster}}.`//home/my-project/output_queues/sink_queue`
SELECT
    string_field || "_processed" AS processed_field,
    int64_field * 2 AS doubled,
    EndsWith(string_field, "bar") AS predicate
FROM
    {{flow-data-cluster}}.`//home/my-project/input_queues/source_queue`
WHERE int64_field > 0;
```

Поддерживается работа с файлами и UDF (встроенными и пользовательскими), лямбда-выражениями и кодогенерацией.

В одном запросе можно комбинировать несколько операций `INSERT INTO ... SELECT`.

{% if audience == "internal" %}

## Чтение и запись из Logbroker {#logbroker}

Источниками и приёмниками данных также могут быть [Logbroker](../../../yandex-specific/flow/extensions/logbroker.md) топики. Для чтения из Logbroker необходимо дополнительно задать прагму `Ytflow.LogbrokerConsumerPath`.

```yql
PRAGMA Ytflow.LogbrokerConsumerPath = "yt/my-project/lb_consumer";

INSERT INTO logbroker.`yt/my-project/lb_sink_topic`
SELECT * FROM logbroker.`yt/my-project/lb_source_topic`;
```

Для разветвления потока на несколько выходов можно использовать конструкцию `PROCESS ... USING`:

```yql
PRAGMA Ytflow.LogbrokerConsumerPath = "yt/my-project/lb_consumer";

-- лямбда разделяет входящие строки на два потока
$lambda = ($row) -> {
    $lb_type = Struct<Data: String>;
    $yt_type = Struct<StringField: String?>;
    $variant_type = Variant<$lb_type, $yt_type>;
    return If(
        StartsWith($row.Data, "foo"),
        Variant($row, "0", $variant_type),
        Variant(AsStruct($row.Data as StringField), "1", $variant_type)
    );
};

$lb_data, $yt_data = process
    logbroker.`yt/my-project/lb_source_topic`
    using $lambda(TableRow());

INSERT INTO logbroker.`yt/my-project/lb_sink_topic`
SELECT * FROM $lb_data;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/output_queues/yt_sink_queue`
SELECT * FROM $yt_data;
```

В одном запросе можно произвольно комбинировать чтения и записи из/в Logbroker и упорядоченные динамические таблицы {{product-name}}.

{% note warning %}

Выходные топики Logbroker и консьюмер входного топика должны быть созданы заранее.

{% endnote %}

{% endif %}

## Джойн потока с динамической таблицей (lookup join) {#lookup-join}

Стрим можно джойнить с сортированной динамической таблицей (key-value таблицей). Поддерживаемые типы джойна для пары «поток + KV таблица»: `LEFT`, `LEFT ONLY`, `LEFT SEMI`, `INNER`. Для пары «KV таблица + поток» типы симметричны.

```yql
$input_stream =
    SELECT key, value, key || "_before" AS key_before
    FROM {{flow-data-cluster}}.`//home/my-project/input_queues/source_queue`
    WHERE value > 2;

$joined_stream =
    SELECT
        left_arg.key AS key,
        left_arg.value AS value,
        left_arg.key_before,
        right_arg.kv_value
    FROM $input_stream AS left_arg
    INNER JOIN
        {{flow-data-cluster}}.`//home/my-project/states/kv_table` AS right_arg
    USING (key);

INSERT INTO {{flow-data-cluster}}.`//home/my-project/output_queues/sink_queue`
SELECT * from $joined_stream
WHERE value * 2 <= kv_value;
```

## Ближайшие планы {#roadmap}

В разработке:
- Агрегации по окнам фиксированного размера (hopping windows)
- Джойн со статическими таблицами
- Джойн по префиксу ключевых колонок
- Джойн нескольких потоков между собой

## См. также

- [Computation (концепция)](../../../flow/concepts/computation.md)
- [Быстрый старт (YQL)](../../../flow/yql/getting-started.md)
