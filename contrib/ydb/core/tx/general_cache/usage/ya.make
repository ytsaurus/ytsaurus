LIBRARY()

SRCS(
    abstract.cpp
    events.cpp
    config.cpp
    service.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/general_cache/service
    contrib/ydb/core/tx/general_cache/source
)

END()
