LIBRARY()

SRCS(
    distributed_retro_collector.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/nodewarden
    contrib/ydb/core/protos
    contrib/ydb/library/actors/retro_tracing
)

END()
