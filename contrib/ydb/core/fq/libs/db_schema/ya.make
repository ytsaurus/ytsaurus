LIBRARY()

SRCS(
    db_schema.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
