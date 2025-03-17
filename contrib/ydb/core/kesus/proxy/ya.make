LIBRARY()

SRCS(
    events.cpp
    proxy.cpp
    proxy_actor.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/erasure
    contrib/ydb/core/kesus/tablet
    contrib/ydb/core/scheme
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/library/services
    contrib/ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
