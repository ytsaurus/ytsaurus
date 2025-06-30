LIBRARY()

SRCS(
    events.cpp
    config.cpp
    service.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/services/metadata/request
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
