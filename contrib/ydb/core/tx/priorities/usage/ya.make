LIBRARY()

SRCS(
    abstract.cpp
    events.cpp
    config.cpp
    service.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/protos
)

END()
