DOCS()

DOCS_VARS(
    version=0
    audience=public
    product-name=YTsaurus
    source-root=https://github.com/ytsaurus/ytsaurus/tree/main
    prestable-cluster=<cluster-name>
    testing-cluster=<cluster-name>
    production-cluster=<cluster-name>
)

DOCS_DIR(
    yt/docs
)

DOCS_INCLUDE_SOURCES(
    # Python examples
    yt/python/examples/simple_map/example.py
    yt/python/examples/simple_reduce/example.py
    yt/python/examples/multiple_input_reduce/example.py
    yt/python/examples/multiple_input_multiple_output_reduce/example.py
    yt/python/examples/table_read_write/example.py
    yt/python/examples/map_reduce/example.py
    yt/python/examples/batch_client/example.py
    yt/python/examples/dynamic_tables_rpc/example.py
    yt/python/examples/simple_rpc/example.py
    yt/python/examples/table_switches/example.py
    yt/python/examples/gevent/example.py
    yt/python/examples/yson_string_proxy/example.py
    yt/python/examples/files/example.py
    yt/python/examples/job_decorators/example.py
    yt/python/examples/dataclass_typed/example.py
    yt/python/examples/files_typed/example.py
    yt/python/examples/grep_typed/example.py
    yt/python/examples/job_decorators_typed/example.py
    yt/python/examples/map_reduce_typed/example.py
    yt/python/examples/map_reduce_multiple_intermediate_streams_typed/example.py
    yt/python/examples/multiple_input_multiple_output_reduce_typed/example.py
    yt/python/examples/multiple_input_reduce_typed/example.py
    yt/python/examples/prepare_operation_typed/example.py
    yt/python/examples/simple_map_typed/example.py
    yt/python/examples/simple_reduce_typed/example.py
    yt/python/examples/spec_builder_typed/example.py
    yt/python/examples/table_read_write_typed/example.py
    yt/python/examples/table_schema_typed/example.py
    yt/python/examples/table_switches_typed/example.py


    # C++ examples
    yt/cpp/mapreduce/examples/tutorial/simple_map_tnode/main.cpp
    yt/cpp/mapreduce/examples/tutorial/simple_map_protobuf/main.cpp
    yt/cpp/mapreduce/examples/tutorial/simple_map_protobuf/data.proto
    yt/cpp/mapreduce/examples/tutorial/simple_map_lambda/main.cpp
    yt/cpp/mapreduce/examples/tutorial/simple_reduce_tnode/main.cpp
    yt/cpp/mapreduce/examples/tutorial/protobuf_complex_types/data.proto
    yt/cpp/mapreduce/examples/tutorial/protobuf_complex_types/main.cpp
    yt/cpp/mapreduce/examples/tutorial/multiple_input_reduce_tnode/main.cpp
    yt/cpp/mapreduce/examples/tutorial/map_tnode_with_file/main.cpp
    yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_tnode/main.cpp
    yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_protobuf/main.cpp
    yt/cpp/mapreduce/examples/tutorial/multiple_input_multiple_output_reduce_protobuf/data.proto
    yt/cpp/mapreduce/examples/tutorial/table_read_write_tnode/main.cpp
    yt/cpp/mapreduce/examples/tutorial/stateful_map_tnode/main.cpp
    yt/cpp/mapreduce/examples/tutorial/mapreduce_protobuf/main.cpp
    yt/cpp/mapreduce/examples/tutorial/mapreduce_lambda/main.cpp
    yt/cpp/mapreduce/examples/tutorial/operation_tracker/main.cpp
    yt/cpp/mapreduce/examples/tutorial/join_reduce_tnode/main.cpp
    yt/cpp/mapreduce/examples/tutorial/batch_request/main.cpp
    yt/cpp/mapreduce/examples/tutorial/pass_table_as_file/main.cpp
    yt/cpp/mapreduce/examples/tutorial/job_statistics/main.cpp
    yt/cpp/mapreduce/examples/tutorial/dyntable_get_insert/main.cpp
    yt/cpp/mapreduce/examples/tutorial/prepare_operation/main.cpp
    yt/cpp/mapreduce/examples/tutorial/prepare_operation/grepper.proto

    # Java SDK examples
    yt/java/ytsaurus-client-examples/cypress-operations-example/src/main/java/tech/ytsaurus/example/ExampleCypressOperations.java
    yt/java/ytsaurus-client-examples/dynamic-table-example/src/main/java/tech/ytsaurus/example/ExampleDynamicTable.java
    yt/java/ytsaurus-client-examples/map-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapEntity.java
    yt/java/ytsaurus-client-examples/map-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapYTree.java
    yt/java/ytsaurus-client-examples/map-reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceEntity.java
    yt/java/ytsaurus-client-examples/map-reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleMapReduceYTree.java
    yt/java/ytsaurus-client-examples/reduce-entity-example/src/main/java/tech/ytsaurus/example/ExampleReduceEntity.java
    yt/java/ytsaurus-client-examples/reduce-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReduceYTree.java
    yt/java/ytsaurus-client-examples/read-write-entity-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteEntity.java
    yt/java/ytsaurus-client-examples/read-write-ytree-example/src/main/java/tech/ytsaurus/example/ExampleReadWriteYTree.java

    # Spark examples
    yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/SmokeTest.java
    yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/UdfExample.java
    yt/spark/spark-over-yt/java-examples/src/main/java/tech/ytsaurus/spyt/example/GroupingExample.java
)

END()

RECURSE(
    ytsaurus
)

IF (NOT OPENSOURCE)
    RECURSE(
        yandex-specific
    )
ENDIF()
