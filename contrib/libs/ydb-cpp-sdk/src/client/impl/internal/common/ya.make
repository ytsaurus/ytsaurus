LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    balancing_policies.cpp
    parser.cpp
    getenv.cpp
    client_pid.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/uri
    contrib/libs/ydb-cpp-sdk/src/client/types/exceptions
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    contrib/libs/ydb-cpp-sdk/src/library/issue
)

END()
