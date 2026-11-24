LIBRARY(sdk-library-grpc-client-v3)

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    grpc_client_low.cpp
    grpc_common.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/containers/stack_vector
    library/cpp/openssl/holders
    contrib/libs/ydb-cpp-sdk/src/library/time
)

END()
