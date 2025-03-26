LIBRARY()

SRCS(
    tx_change_blobs_owning.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/tiering
    contrib/ydb/services/metadata/abstract
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/blobs_action/protos
    contrib/ydb/core/base
    contrib/ydb/core/tx/tiering
)

END()
