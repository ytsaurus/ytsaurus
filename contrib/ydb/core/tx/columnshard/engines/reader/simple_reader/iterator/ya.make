LIBRARY()

SRCS(
    scanner.cpp
    source.cpp
    fetched_data.cpp
    plain_read_data.cpp
    context.cpp
    fetching.cpp
    iterator.cpp
    collections.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/limiter/grouped_memory/usage
)

END()
