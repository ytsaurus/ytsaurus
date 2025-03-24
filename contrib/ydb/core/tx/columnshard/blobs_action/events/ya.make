LIBRARY()

SRCS(
    delete_blobs.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/blobs_action/protos
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/datashard
)

END()
