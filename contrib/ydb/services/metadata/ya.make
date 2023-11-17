LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/manager
)

END()

RECURSE(
    secret
    initializer
)