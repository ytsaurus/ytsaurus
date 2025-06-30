# Отличия в SQL диалектах ClickHouse и YQL

{% if audience == "internal" %}
{% note warning %}

Если вы одновременно хорошо знакомы и с ClickHouse и с YQL, то не стесняйтесь, пожалуйста, дополнять данную статью наиболее значимыми отличиями [вот здесь]({{source-root}}/yt/docs/ru/yql/differences_from/clickhouse.md).

{% endnote %}
{% endif %}

## Терминология
* `ARRAY` из ClickHouse называется `List` в YQL, соответствующим образом отличается большинство работающих с ними функций.
* `ARRAY JOIN` из ClickHouse в YQL называется [FLATTEN BY](../syntax/flatten.md).
* С точки зрения YQL, тип данных логической таблицы — `List<Struct<...>>`, как физической, так и вложенной (Nested из ClickHouse). В YQL можно комбинировать [контейнерные типы данных](../types/containers.md) произвольным образом.

## Синтаксис
* В ClickHouse можно дать практически любой части выражения имя через `AS` и затем использовать его в другой части запроса, а в YQL всё очень строго с областью видимости колонок в различных частях [SELECT](../syntax/select/index.md).
