LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/initializer
    contrib/ydb/services/metadata/abstract
    contrib/ydb/core/kqp/gateway/actors
    contrib/ydb/core/kqp/gateway/behaviour/tablestore/operations
)

YQL_LAST_ABI_VERSION()

END()
