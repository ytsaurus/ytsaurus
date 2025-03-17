LIBRARY()

SRCS(
    builder.cpp
    merger.cpp
    restore.cpp
)

PEERDIR(
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/data_events
    contrib/ydb/core/formats/arrow/reader
    contrib/ydb/library/conclusion
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
    contrib/ydb/core/tx/columnshard/data_reader
    contrib/ydb/core/tx/columnshard/engines/writer
    contrib/ydb/core/tx/data_events/common
    contrib/ydb/core/tx/columnshard/engines/scheme
)

END()
