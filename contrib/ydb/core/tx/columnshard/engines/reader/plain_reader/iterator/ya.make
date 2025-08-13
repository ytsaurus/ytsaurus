LIBRARY()

SRCS(
    constructors.cpp
    scanner.cpp
    source.cpp
    interval.cpp
    fetched_data.cpp
    plain_read_data.cpp
    merge.cpp
    context.cpp
    fetching.cpp
    iterator.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/limiter/grouped_memory/usage
)

END()
