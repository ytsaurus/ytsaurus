LIBRARY()

SRCS(
    manager.cpp
    counters.cpp
    service.cpp
)

PEERDIR(
    contrib/ydb/core/protos
)

END()
