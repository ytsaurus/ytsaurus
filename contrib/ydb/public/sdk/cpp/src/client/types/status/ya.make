LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/plain_status
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/src/library/issue
)

END()
