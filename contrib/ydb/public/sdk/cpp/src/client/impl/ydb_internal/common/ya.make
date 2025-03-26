LIBRARY()

SRCS(
    parser.cpp
    getenv.cpp
    client_pid.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/library/issue
)

END()
