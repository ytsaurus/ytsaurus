LIBRARY()

SRCS(
    service.cpp
    config.cpp
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
)

END()
