LIBRARY()

SRCS(
    manager.cpp
    object.cpp
    initializer.cpp
    checker.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/initializer
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/secret
    contrib/ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
