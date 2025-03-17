LIBRARY()

PEERDIR(
    contrib/ydb/library/grpc/server
    contrib/ydb/core/base
    contrib/ydb/core/client/scheme_cache_lib
    contrib/ydb/core/client/server
    contrib/ydb/core/engine
    contrib/ydb/public/lib/deprecated/kicli
)

END()

RECURSE(
    metadata
    minikql_compile
    scheme_cache_lib
    server
)

RECURSE_FOR_TESTS(
    ut
)
