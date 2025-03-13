LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    extension.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    contrib/ydb/public/sdk/cpp/src/client/driver
)

END()
