LIBRARY()

SRCS(
    conveyor_task.cpp
    description.cpp
    queue.cpp
    read_filter_merger.cpp
    read_metadata.cpp
    read_context.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/predicate
    contrib/ydb/core/tx/columnshard/hooks/abstract
    contrib/ydb/core/tx/columnshard/resources
    contrib/ydb/core/tx/program
    contrib/ydb/core/tx/columnshard/engines/reader/plain_reader
    contrib/ydb/core/tx/columnshard/engines/scheme
)

GENERATE_ENUM_SERIALIZATION(read_metadata.h)
YQL_LAST_ABI_VERSION()

END()
