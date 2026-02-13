LIBRARY()

SRCS(
    balancing_policies.cpp
    parser.cpp
    getenv.cpp
    client_pid.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/types/exceptions
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/library/issue
    library/cpp/uri
    library/cpp/cgiparam
)

END()
