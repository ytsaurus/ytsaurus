# MATERIALIZE

Материализует указанный источник или выражение на текущем или заданном кластере. В случае материализации выражения его тип должен быть списком структур. Кластер для материализации берется из выражения `ON` или, при его отсутствии, из оператора [USE](use.md).
Материализованный источник сохраняет все колонки и порядок сортировки. Он также создает барьер, который не позволяет оптимизаторам объединять вычисления по разные стороны материализации.

## Синтаксис {#syntax}

```yql
MATERIALIZE
    <source>        -- имя таблицы, именованное выражение или вложенный SELECT
INTO $<bind_name>   -- имя параметра для обращения к результату материализации
ON <cluster>        -- кластер, на котором материализуется источник (опционально)
WITH <hints>        -- дополнительные модификаторы (опционально)
```

## Доступность {#availability}

`MATERIALIZE` доступен начиная с версии языка [2026.02](../changelog/2026.02.md).

В Query Tracker для использования `MATERIALIZE` выберите версию языка 2026.02 или новее.

## Модификаторы {#modifiers}

Модификатор указывается после ключевого слова `WITH`. Значение отделяется знаком `=`. Несколько модификаторов заключаются в круглые скобки: `WITH (SOME_HINT1=value, SOME_HINT2)`.

Поддерживается модификатор `prune_unused_columns`: он удаляет из материализованного источника колонки, которые не используются потребителями. Системы, на кластерах которых выполняется материализация, могут поддерживать дополнительные модификаторы.

## Примеры {#examples}

```yql
USE cluster;

MATERIALIZE (SELECT 1 AS a, 2 AS b) INTO $materialized;

SELECT * FROM $materialized;
```

```yql
USE cluster;

$input = SELECT key, value FROM my_table ORDER BY key;

MATERIALIZE $input INTO $materialized ON another_cluster;

SELECT * FROM another_table AS a
JOIN $materialized AS b USING key;
```

Материализованный источник во втором примере сохраняет сортировку по `key`, поэтому выбор стратегии `JOIN` может учитывать эту сортировку.

```yql
USE cluster;

$input = SELECT a, b, c, d FROM my_table;

MATERIALIZE $input INTO $materialized WITH prune_unused_columns;
SELECT a, b FROM $materialized;
SELECT c FROM $materialized;
```

После оптимизации последний пример материализует только колонки `a`, `b` и `c`.
