LIBRARY()

SRCS(
    events.cpp
    config.cpp
    abstract.cpp
    service.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/services/metadata/request
)

END()
