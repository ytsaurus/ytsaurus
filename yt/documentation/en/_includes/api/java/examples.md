# Java interface with examples (ytsaurus-client)

We have Java SDK for YTsaurus which is published in [maven](https://central.sonatype.com/artifact/tech.ytsaurus/ytsaurus-client/1.0.1).

A detailed description of the Java SDK is located [here](https://javadoc.io/doc/tech.ytsaurus/ytsaurus-client/latest/index.html).

The examples are located at [yt/java/ytsaurus-client-examples](https://github.com/ytsaurus/ytsaurus/tree/main/yt/java/ytsaurus-client-examples).

Before using the examples, read the [instructions for obtaining the token](../../../user-guide/storage/auth.md).

Also you need to have tutorial tables on your YTsaurus cluster. It can be generated using this script:
```bash
./yt/documentation/upload_tutorial_data/upload_tutorial_data.py -gw --proxy <your-http-proxy>
```

Every example can be run from its directory using gradle:
```bash
cd yt/java/ytsaurus-client-examples/<some-example>
YT_PROXY=<your-http-proxy> gradle run
```

### Working with Cypress

The example is located at [yt/java/ytsaurus-client-examples/cypress-operations-example/src/main/java/tech/ytsaurus/example/ExampleCypressOperations.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/cypress-operations-example/src/main/java/tech/ytsaurus/example/ExampleCypressOperations.java).

{% code '/yt/java/ytsaurus-client-examples/cypress-operations-example/src/main/java/tech/ytsaurus/example/ExampleCypressOperations.java' lang='java' %}

### Reading and writing static tables (Entity version)

The example is located at [yt/java/ytsaurus-client-examples/read-write-entity-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/read-write-entity-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteEntity.java).

{% code '/yt/java/ytsaurus-client-examples/read-write-entity-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteEntity.java' lang='java' %}

### Reading and writing static tables (YTreeMapNode version)

The example is located at [yt/java/ytsaurus-client-examples/read-write-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteYTree.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/read-write-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteYTree.java).

{% code '/yt/java/ytsaurus-client-examples/read-write-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteYTree.java' lang='java' %}

### Working with dynamic tables

The example is located at [yt/java/ytsaurus-client-examples/dynamic-table-example/src/main/java/tech/ytsaurus/example/ExampleDynamicTable.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/dynamic-table-example/src/main/java/tech/ytsaurus/example/ExampleDynamicTable.java).

{% code '/yt/java/ytsaurus-client-examples/dynamic-table-example/src/main/java/tech/ytsaurus/example/ExampleDynamicTable.java' lang='java' %}

### Map operation (UserClass version)

The example is located at [yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java).

{% code '/yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java' lang='java' %}

### Map operation (YTreeMapNode version)

The example is located at [yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java).

{% code '/yt/java/ytsaurus-client-examples/map-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapYTree.java' lang='java' %}

### Reduce operation (UserClass version)

The example is located at [yt/java/ytsaurus-client-examples/reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleReduceEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleReduceEntity.java).

{% code '/yt/java/ytsaurus-client-examples/reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleReduceEntity.java' lang='java' %}

### Reduce operation (YTreeMapNode version)

The example is located at [yt/java/ytsaurus-client-examples/reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReduceYTree.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReduceYTree.java).

{% code '/yt/java/ytsaurus-client-examples/reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReduceYTree.java' lang='java' %}

### MapReduce operation (UserClass version)

The example is located at [yt/java/ytsaurus-client-examples/map-reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceEntity.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceEntity.java).

{% code '/yt/java/ytsaurus-client-examples/map-reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceEntity.java' lang='java' %}

### MapReduce operation (YTreeMapNode version)

The example is located at [yt/java/ytsaurus-client-examples/map-reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceYTree.java](https://github.com/ytsaurus/ytsaurus/blob/main/yt/java/ytsaurus-client-examples/map-reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceYTree.java).

{% code '/yt/java/ytsaurus-client-examples/map-reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceYTree.java' lang='java' %}
