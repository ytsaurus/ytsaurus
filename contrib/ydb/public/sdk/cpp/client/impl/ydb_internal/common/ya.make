LIBRARY()

SRCS(
    parser.cpp
    getenv.cpp
    string_helpers.cpp
    client_pid.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/client
    contrib/ydb/library/yql/public/issue
)

END()
