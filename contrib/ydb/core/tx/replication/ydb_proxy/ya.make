LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
    contrib/ydb/public/sdk/cpp/src/client/types/credentials/login
)

SRCS(
    topic_message.cpp
    ydb_proxy.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    local_proxy
)

RECURSE_FOR_TESTS(
    ut
)
