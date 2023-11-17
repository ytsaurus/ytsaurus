LIBRARY()

SRCS(
    manager.cpp
    object.cpp
    GLOBAL behaviour.cpp
    initializer.cpp
    checker.cpp
    ss_checker.cpp
    GLOBAL ss_fetcher.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/common
    contrib/ydb/services/metadata/initializer
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
