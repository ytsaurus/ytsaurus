LIBRARY()

SRCS(
    service.cpp
    manager.cpp
    counters.cpp
)

PEERDIR(
    contrib/ydb/core/tx/priorities/usage
    contrib/ydb/core/protos
)

END()
