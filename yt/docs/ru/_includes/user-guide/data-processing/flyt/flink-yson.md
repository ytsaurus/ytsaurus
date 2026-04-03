# Apache Flink YSON Formatter

Apache Flink YSON Formatter — это форматер, позволяющий использовать YSON формат в задачах потоковой обработки данных на Apache Flink.

## Установка

{% note info %}

Текущая версия форматера требует:
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

## Исходный код

Исходный код доступен на [странице проекта на GitHub](https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-yson).
