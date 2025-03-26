LIBRARY()

SRCS(
    write.cpp
    write_data.cpp
    manager.cpp
    events.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/data_events
    contrib/ydb/services/metadata
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
    contrib/ydb/core/tx/columnshard/data_reader
    contrib/ydb/core/tx/columnshard/transactions/locks
    contrib/ydb/core/tx/columnshard/operations/batch_builder
    contrib/ydb/core/tx/columnshard/operations/slice_builder
    contrib/ydb/core/tx/columnshard/operations/common
)

END()
