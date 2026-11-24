LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    settings.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/endpoints
    contrib/libs/ydb-cpp-sdk/src/library/time
)

END()
