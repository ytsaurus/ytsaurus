LIBRARY(ydb_sdk_core_access)

SRCS(
    ../ydb_sdk_core_access.cpp
)

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/types
)

END()
