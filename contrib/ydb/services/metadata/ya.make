LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/manager
)

END()

RECURSE(
    initializer
    secret
)
