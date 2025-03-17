LIBRARY()

SRCS(
    service.cpp
    config.cpp
    events.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
)

END()
