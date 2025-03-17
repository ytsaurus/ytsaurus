LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/federated_query
    contrib/ydb/core/kqp/gateway/actors
    contrib/ydb/core/kqp/gateway/utils

    contrib/ydb/library/conclusion

    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/initializer
    contrib/ydb/services/metadata/secret
)

YQL_LAST_ABI_VERSION()

END()
