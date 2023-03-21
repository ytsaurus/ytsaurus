# Java интерфейс на примерах (ytsaurus-client)

Java API опубликовано в [maven](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.0.1).

Документацию можно найти [здесь](https://javadoc.io/doc/tech.ytsaurus/ytsaurus-client/latest/index.html).

Примеры находятся в [yt/java/ytsaurus-client-examples](https://github.com/ytsaurus/ytsaurus/tree/main/yt/java/ytsaurus-client-examples).

Перед использованием примеров прочитайте [инструкцию по получению токена](../../../user-guide/storage/auth.md).

Примеры можно запускать с помощью gradle из директории примера:
```bash
cd yt/java/ytsaurus-client-examples/<some-example>
YT_PROXY=<your-http-proxy> gradle run
```

### Работа с Кипарисом

Пример находится в [yt/java/ytsaurus-client-examples/cypress-operations-example/src/main/java/tech/ytsaurus/example/ExampleCypressOperations.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/cypress-operations-example/src/main/java/tech/ytsaurus/example/ExampleCypressOperations.java).

{% code '/yt/java/ytsaurus-client-examples/cypress-operations-example/src/main/java/tech/ytsaurus/example/ExampleCypressOperations.java' lang='java' %}


### Чтение и запись статических таблиц (UserClass версия)

Пример находится в [yt/java/ytsaurus-client-examples/read-write-entity-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/read-write-entity-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteEntity.java).

{% code '/yt/java/ytsaurus-client-examples/read-write-entity-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteEntity.java' lang='java' %}

### Чтение и запись статических таблиц (YTreeMapNode версия)

Пример находится в [yt/java/ytsaurus-client-examples/read-write-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteYTree.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/read-write-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteYTree.java).

{% code '/yt/java/ytsaurus-client-examples/read-write-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteYTree.java' lang='java' %}

### Работа с динамическими таблицами

Пример находится в [yt/java/ytsaurus-client-examples/dynamic-table-example/src/main/java/tech/ytsaurus/example/ExampleDynamicTable.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/dynamic-table-example/src/main/java/tech/ytsaurus/example/ExampleDynamicTable.java).

{% code '/yt/java/ytsaurus-client-examples/dynamic-table-example/src/main/java/tech/ytsaurus/example/ExampleDynamicTable.java' lang='java' %}

### Map операция (UserClass версия)

Пример находится в [yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java).

{% code '/yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java' lang='java' %}

### Map operation (YTreeMapNode version)

The example is located at [yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java).

{% code '/yt/java/ytsaurus-client-examples/map-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapYTree.java' lang='java' %}

### Reduce операция (UserClass версия)

Пример находится в [yt/java/ytsaurus-client-examples/reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleReduceEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleReduceEntity.java).

{% code '/yt/java/ytsaurus-client-examples/reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleReduceEntity.java' lang='java' %}

### Reduce операция (YTreeMapNode версия)

Пример находится в [yt/java/ytsaurus-client-examples/reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReduceYTree.jav](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReduceYTree.java).

{% code '/yt/java/ytsaurus-client-examples/reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReduceYTree.java' lang='java' %}

### MapReduce операция (UserClass версия)

Пример находится в [yt/java/ytsaurus-client-examples/map-reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceEntity.java).

{% code '/yt/java/ytsaurus-client-examples/map-reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceEntity.java' lang='java' %}

### MapReduce операция (YTreeMapNode версия)

Пример находится в [yt/java/ytsaurus-client-examples/map-reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceYTree.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceYTree.java).

{% code '/yt/java/ytsaurus-client-examples/map-reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceYTree.java' lang='java' %}
