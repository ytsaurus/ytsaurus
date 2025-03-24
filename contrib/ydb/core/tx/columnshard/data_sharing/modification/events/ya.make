LIBRARY()

SRCS(
    change_owning.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/blobs_action/protos
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/datashard
)

END()
