LIBRARY()

SRCS(
    worker.cpp
    service.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/conveyor/tracing
)

END()
