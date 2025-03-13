LIBRARY()

SRCS(
    params.cpp
    impl.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
