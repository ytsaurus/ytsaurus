LIBRARY()

SRCS(
    acquire_snapshot_impl.cpp
    commit_impl.cpp
    long_tx_service.cpp
    long_tx_service_impl.cpp
    lwtrace_probes.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    contrib/ydb/core/base
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    public
    public/ut
    ut
)

RECURSE_FOR_TESTS(
    ut
)
