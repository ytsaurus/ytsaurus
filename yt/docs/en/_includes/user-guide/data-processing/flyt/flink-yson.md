# Apache Flink YSON Formatter

Apache Flink YSON Formatter is a formatter that allows you to use the YSON format in streaming data processing tasks on Apache Flink.

## Installation

{% note info %}

The current version of the formatter requires:
- Java 11
- Apache Flink 1.20

{% endnote %}

Maven

```xml
<dependency>
    <groupId>tech.ytsaurus.flyt.formats.yson</groupId>
    <artifactId>flink-yson</artifactId>
    <version>${formatterVersion}</version>
    <classifier>all</classifier>
</dependency>
```

Gradle

```kotlin
implementation("tech.ytsaurus.flyt.formats.yson:flink-yson:$formatterVersion:all")
```

## Source code

The source code is available on the [project page on GitHub](https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-yson).