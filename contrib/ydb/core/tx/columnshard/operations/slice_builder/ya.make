LIBRARY()

SRCS(
    builder.cpp
    pack_builder.cpp
)

PEERDIR(
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/data_events
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/tx/columnshard/engines/writer
)

END()
