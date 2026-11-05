LIBRARY(sdk-library-grpc-client-v3)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    grpc_client_low.cpp
    grpc_common.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/containers/stack_vector
    library/cpp/openssl/holders
    contrib/ydb/public/sdk/cpp/src/library/time
)

END()
