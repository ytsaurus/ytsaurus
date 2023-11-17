LIBRARY()

SRCS(
    scanner.cpp
    source.cpp
    constructor.cpp
    interval.cpp
    fetched_data.cpp
    plain_read_data.cpp
    filter_assembler.cpp
    column_assembler.cpp
    committed_assembler.cpp
    columns_set.cpp
    context.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/blobs_action
    contrib/ydb/core/tx/conveyor/usage
)

END()
