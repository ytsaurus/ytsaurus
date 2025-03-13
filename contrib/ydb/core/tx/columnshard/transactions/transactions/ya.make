LIBRARY()

SRCS(
    tx_add_sharding_info.cpp
    tx_finish_async.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/abstract
    contrib/ydb/core/tx/columnshard/blobs_action/protos
    contrib/ydb/core/tx/columnshard/data_sharing/protos
)

END()
