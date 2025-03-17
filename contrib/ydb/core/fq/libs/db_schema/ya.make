LIBRARY()

SRCS(
    db_schema.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
