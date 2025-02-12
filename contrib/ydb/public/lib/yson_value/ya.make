LIBRARY()

SRCS(
    ydb_yson_value.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/public/sdk/cpp/src/client/result
    contrib/ydb/public/sdk/cpp/src/client/value
    yql/essentials/types/uuid
)

END()
