# Apache Flink Connector {{product-name}}

Apache Flink Connector {{product-name}} is a connector that allows you to use {{product-name}} in streaming data processing tasks on Apache Flink.

The connector works with [sorted dynamic tables](../../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) in {{product-name}}. With it, you can perform streaming writes to sorted dynamic tables in {{product-name}}, as well as perform Lookup operations.

Key features:

- Writing to sorted dynamic tables;
- Data partitioning support;
- Automatic table creation;
- Preliminary [table resharding](../../../../user-guide/dynamic-tables/resharding.md);
- Support for synchronous and asynchronous Lookup operations;
- Full and Partial caching strategies for Lookup queries.

## Installation

{% note info %}

The current version of the connector requires:
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

## Source code

The source code is available on the [project page on GitHub](https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-connector-ytsaurus).

## Getting started

You can learn more about working with the connector through the [Quick Start Guide](https://github.com/ytsaurus/ytsaurus-flyt/blob/main/flink-connector-ytsaurus/README.md#quick-start-guide) on the project page.