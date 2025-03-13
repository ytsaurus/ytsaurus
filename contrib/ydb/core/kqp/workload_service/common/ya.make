LIBRARY()

SRCS(
    cpu_quota_manager.cpp
    events.cpp
    helpers.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common/events

    contrib/ydb/core/scheme

    contrib/ydb/core/tx/scheme_cache

    contrib/ydb/library/actors/core

    contrib/ydb/public/sdk/cpp/src/client/types

    library/cpp/retry
)

YQL_LAST_ABI_VERSION()

END()
