LIBRARY()

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_service.cpp
    kqp_compile_computation_pattern_service.cpp
)

PEERDIR(
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/host
    contrib/ydb/core/ydb_convert
)

YQL_LAST_ABI_VERSION()

END()
