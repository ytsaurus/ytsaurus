LIBRARY()

SRCS(
    db_async_resolver_impl.cpp
    mdb_endpoint_generator.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/core/fq/libs/events
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/dq/actors
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

