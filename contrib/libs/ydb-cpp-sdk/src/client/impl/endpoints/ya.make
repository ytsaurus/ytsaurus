LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    contrib/ydb/public/api/grpc
)

END()
