LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
)

PEERDIR(
    library/cpp/retry
    library/cpp/threading/future
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/protos
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/core
    contrib/ydb/services/metadata/secret
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_service
)
