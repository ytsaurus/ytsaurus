LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/sdk/cpp/client/ydb_topic
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials
    contrib/ydb/public/sdk/cpp/client/ydb_types/credentials/login
)

SRCS(
    ydb_proxy.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
