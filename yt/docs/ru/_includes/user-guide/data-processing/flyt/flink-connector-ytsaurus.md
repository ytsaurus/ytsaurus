# Apache Flink Connector {{product-name}}

Apache Flink Connector {{product-name}} — это коннектор, позволяющий использовать {{product-name}} в задачах потоковой обработки данных на Apache Flink.

Коннектор работает с [сортированными динамическими таблицами](../../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) {{product-name}}. С его помощью можно осуществлять потоковую запись в сортированные динамические таблицы {{product-name}}, а также выполнять Lookup-операции.

Ключевые особенности:

- Запись в сортированные динамические таблицы;
- Поддержка партиционирования данных;
- Автоматическое создание таблиц;
- Предварительное [решардирование таблиц](../../../../user-guide/dynamic-tables/resharding.md);
- Поддержка синхронных и асинхронных Lookup операций;
- Наличие Full и Partial стратегий кеширования Lookup запросов.

## Установка

{% note info %}

Текущая версия коннектора требует:
- Java 11
- Apache Flink 1.20

{% endnote %}

Maven

```xml
<dependency>
    <groupId>tech.ytsaurus.flyt.connectors.ytsaurus</groupId>
    <artifactId>flink-connector-ytsaurus</artifactId>
    <version>${connectorVersion}</version>
    <classifier>all</classifier>
</dependency>
```

Gradle

```kotlin
implementation("tech.ytsaurus.flyt.connectors.ytsaurus:flink-connector-ytsaurus:$connectorVersion:all")
```

## Исходный код

Исходный код доступен на [странице проекта на GitHub](https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-connector-ytsaurus).

## Начало работы

Более подробно познакомиться с работой коннектора можно через [Quick Start Guide](https://github.com/ytsaurus/ytsaurus-flyt/blob/main/flink-connector-ytsaurus/README.md#quick-start-guide) на странице проекта.
