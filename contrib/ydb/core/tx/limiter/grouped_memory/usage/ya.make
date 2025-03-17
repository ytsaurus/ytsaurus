LIBRARY()

SRCS(
    events.cpp
    config.cpp
    abstract.cpp
    service.cpp
    stage_features.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/services/metadata/request
    contrib/ydb/core/tx/limiter/grouped_memory/service
)

END()
