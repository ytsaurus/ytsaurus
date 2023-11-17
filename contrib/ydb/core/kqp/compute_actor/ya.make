LIBRARY()

SRCS(
    kqp_compute_actor.cpp
    kqp_compute_actor_helpers.cpp
    kqp_compute_events.cpp
    kqp_compute_state.cpp
    kqp_pure_compute_actor.cpp
    kqp_scan_compute_stat.cpp
    kqp_scan_compute_manager.cpp
    kqp_scan_compute_actor.cpp
    kqp_scan_fetcher_actor.cpp
    kqp_scan_common.cpp
    kqp_scan_events.cpp
)

PEERDIR(
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/tx/datashard
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/providers/generic/actors
    contrib/ydb/library/yql/providers/s3/actors
    contrib/ydb/library/yql/public/issue
)

GENERATE_ENUM_SERIALIZATION(kqp_compute_state.h)
YQL_LAST_ABI_VERSION()

END()
