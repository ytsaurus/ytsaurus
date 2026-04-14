LIBRARY()

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_service.cpp
    kqp_compile_computation_pattern_service.cpp
    kqp_warmup_compile_actor.cpp
)

PEERDIR(
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/kqp/counters
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/host
    contrib/ydb/core/ydb_convert
    contrib/ydb/core/kqp/compile_service/helpers
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/query_actor
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    helpers
)

RECURSE_FOR_TESTS(
    ut
)
