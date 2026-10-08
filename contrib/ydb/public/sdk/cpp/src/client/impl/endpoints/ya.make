LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    contrib/ydb/public/api/grpc
)

END()
