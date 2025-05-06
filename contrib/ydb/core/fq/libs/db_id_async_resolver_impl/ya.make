LIBRARY()

SRCS(
    database_resolver.cpp
    db_async_resolver_impl.cpp
    http_proxy.cpp
    mdb_endpoint_generator.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/threading/future
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/util
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    contrib/ydb/library/services
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/dq/actors
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

