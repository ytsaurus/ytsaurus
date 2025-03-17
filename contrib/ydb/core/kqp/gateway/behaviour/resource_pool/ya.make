LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    contrib/ydb/core/cms/console
    contrib/ydb/core/kqp/gateway/actors
    contrib/ydb/core/kqp/gateway/utils
    contrib/ydb/core/protos
    contrib/ydb/core/resource_pools

    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()
