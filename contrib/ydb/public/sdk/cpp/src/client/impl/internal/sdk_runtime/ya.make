LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    runtime.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
)

END()
