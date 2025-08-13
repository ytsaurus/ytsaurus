LIBRARY()

SRCS(
    scanner.cpp
    source.cpp
    fetched_data.cpp
    plain_read_data.cpp
    context.cpp
    fetching.cpp
    iterator.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sync_points
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view
    contrib/ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/limiter/grouped_memory/usage
)

GENERATE_ENUM_SERIALIZATION(source.h)

END()
