LIBRARY()

SRCS(
    tx_draft.cpp
    tx_write_index.cpp
    tx_gc_indexed.cpp
    tx_remove_blobs.cpp
    tx_blobs_written.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/blobs_action/events
)

END()
