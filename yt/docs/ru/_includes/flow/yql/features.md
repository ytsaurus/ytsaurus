# Поддержанные конструкции YQL в {{product-name}} Flow

{% note info %}

Данная страница описывает конструкции YQL, специфичные для работы с Flow.

Общий синтаксис YQL описан в [документации YQL](../../../yql/index.md).

{% endnote %}

## Построчное преобразование потока (мап) {#map}

Основная конструкция — `INSERT INTO ... SELECT ... FROM`.<br>
При помощи нее можно прочитать [поток](../../../flow/concepts/glossary.md#stream-and-computation) из упорядоченной динамической таблицы (очереди {{product-name}}), применить к нему преобразование и записать результат в другую очередь.

Пример запроса:

```yql
INSERT INTO
    {{flow-data-cluster}}.`//home/my-project/sink_queue`
SELECT
    string_field || "_processed" AS processed_field,
    int64_field * 2 AS doubled,
    EndsWith(string_field, "bar") AS predicate
FROM
    {{flow-data-cluster}}.`//home/my-project/source_queue`
WHERE int64_field > 0;
```

Поддерживается работа с файлами и UDF (встроенными и пользовательскими), лямбда-выражениями и кодогенерацией.

В одном запросе можно комбинировать несколько операций `INSERT INTO ... SELECT`.

## Разветвление потока {#multiple-outputs}

Именованные потоки можно переиспользовать в нескольких `SELECT` запросах для разбиения потока на несколько частей, части можно обрабатывать независимо.

В следующем примере строки входного потока с положительным значением колонки `value` попадут в очередь `positive_queue`, остальные — в очередь `other_queue`:

```yql
$events = SELECT
    key,
    value
FROM {{flow-data-cluster}}.`//home/my-project/source_queue`;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/positive_queue`
SELECT * FROM $events WHERE value > 0;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/other_queue`
SELECT * FROM $events WHERE value <= 0;
```

## Объединение потоков (UNION ALL) {#union-all}

`UNION ALL` объединяет несколько потоков для дальнейшей обработки как одного потока.

В этом примере в выходную очередь пишутся строки из обеих входных очередей:

```yql
$events =
    SELECT
        key,
        value
    FROM {{flow-data-cluster}}.`//home/my-project/first_queue`
    UNION ALL
    SELECT
        key,
        value
    FROM {{flow-data-cluster}}.`//home/my-project/second_queue`;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/sink_queue`
SELECT
    key,
    value + 1 AS value
FROM $events;
```

## Джойн потока с динамической таблицей (lookup join) {#lookup-join}

Поток можно джойнить с сортированной динамической таблицей (key value таблицей).<br>
Поддерживаемые типы джойна для пары «поток + KV таблица»: `LEFT`, `LEFT ONLY`, `LEFT SEMI`, `INNER`.<br>
Для пары «KV таблица + поток» типы симметричны.

```yql
$input_stream =
    SELECT key, value, key || "_before" AS key_before
    FROM {{flow-data-cluster}}.`//home/my-project/source_queue`
    WHERE value > 2;

$joined_stream =
    SELECT
        left_arg.key AS key,
        left_arg.value AS value,
        left_arg.key_before,
        right_arg.kv_value
    FROM $input_stream AS left_arg
    INNER JOIN
        {{flow-data-cluster}}.`//home/my-project/kv_table` AS right_arg
    USING (key);

INSERT INTO {{flow-data-cluster}}.`//home/my-project/sink_queue`
SELECT * from $joined_stream
WHERE value * 2 <= kv_value;
```

В одном запросе можно последовательно приджойнить несколько key value таблиц, например:

```yql
INSERT INTO {{flow-data-cluster}}.`//home/my-project/enriched_queue`
SELECT
    events.key AS key,
    events.value AS value,
    first_table.value AS first_value,
    second_table.value AS second_value
FROM {{flow-data-cluster}}.`//home/my-project/source_queue` AS events
LEFT JOIN {{flow-data-cluster}}.`//home/my-project/first_table` AS first_table
    ON events.key = first_table.key
LEFT JOIN {{flow-data-cluster}}.`//home/my-project/second_table` AS second_table
    ON events.key = second_table.key;
```

## Агрегации по окнам (GROUP BY ... HOP) {#group-by-hop}

Агрегаты за интервалы времени фиксированной длины (окна) можно вычислять при помощи конструкции `GROUP BY ... HOP`.

Параметры `HOP` задают время события, сетку начала окон, длительность окон и задержку закрытия окон для опаздывающих событий.
Помимо `HOP`, нужно указать хотя бы один дополнительный ключ группировки.

Общий синтаксис описан в [справочнике YQL](../../../yql/syntax/group_by.md#group-by-hop).

Окно закрывается по [вотермарку](../../../flow/concepts/watermarks.md#event-watermark) времени событий, после пересечения им границы завершения окна, увеличенной на задержку закрытия окна.<br>
События, по своему времени принадлежащие окну, но пришедшие после его закрытия (опоздавшие события) отбрасываются.

Поддерживаются агрегатные функции без модификатора `DISTINCT`.

В следующем примере для каждого ключа и окна вычисляются сумма значений `value` и число строк.<br>
Окна начинаются каждые 15 секунд, длятся 30 секунд и закрываются через минуту после завершения окна.

```yql
INSERT INTO {{flow-data-cluster}}.`//home/my-project/window_totals`
SELECT
    HOP_START() AS window_start,
    key,
    COUNT(*) AS record_count,
    SUM(value) AS value_total
FROM {{flow-data-cluster}}.`//home/my-project/source_queue`
GROUP BY key, HOP(ts, "PT15S", "PT30S", "PT1M");
```

## Запись в сортированную динамическую таблицу {#replace-into}

Запись в сортированную динамическую таблицу поддерживается при помощи `REPLACE INTO ... WITH PRIMARY_KEY = ...`.

При записи новая строка полностью заменяет прежнюю строку с тем же ключом.

Значение хинта `PRIMARY_KEY` должно быть yson списком из одной или более ключевых колонок.<br>
Если выходная таблица уже существует, её невычисляемые ключевые колонки и их порядок должны совпадать
с указанными ключевыми колонками в `PRIMARY_KEY`.<br>
Если же выходной таблицы не существует, то она будет создана перед стартом запроса.

Выходные таблицы с сортировкой ключей по убыванию не поддерживаются.

Пример:

```yql
REPLACE INTO {{flow-data-cluster}}.`//home/my-project/window_totals_table`
WITH PRIMARY_KEY = '[key;window_start]'
SELECT
    key,
    window_start,
    record_count
FROM {{flow-data-cluster}}.`//home/my-project/window_totals`;
```

## Работа с несколькими кластерами {#multiple-clusters}

Входные и выходные таблицы могут находиться на разных кластерах {{product-name}}.<br>
`Ytflow.Cluster` задаёт кластер внутренних таблиц пайплайна, и не ограничивает расположение всех источников и приёмников.

В этом примере `source_cluster` и `destination_cluster` могут отличаться от значения `Ytflow.Cluster`:

```yql
INSERT INTO destination_cluster.`//home/my-project/sink_queue`
SELECT
    key,
    value
FROM source_cluster.`//home/my-project/source_queue`;
```

## См. также

- [Computation (концепция)](../../../flow/concepts/computation.md)
- [Быстрый старт (YQL)](../../../flow/yql/getting-started.md)
