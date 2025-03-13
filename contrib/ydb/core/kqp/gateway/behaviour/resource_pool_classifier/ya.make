LIBRARY()

SRCS(
    GLOBAL behaviour.cpp
    checker.cpp
    fetcher.cpp
    initializer.cpp
    manager.cpp
    object.cpp
    snapshot.cpp
)

PEERDIR(
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp/workload_service/actors
    contrib/ydb/core/protos
    contrib/ydb/core/resource_pools
    contrib/ydb/library/query_actor
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/initializer
    contrib/ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()
