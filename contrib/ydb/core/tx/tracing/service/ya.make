LIBRARY()

SRCS(
    global.cpp
    actor.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/tracing/usage
)

END()
