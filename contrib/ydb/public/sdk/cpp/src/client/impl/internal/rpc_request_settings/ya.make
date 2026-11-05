LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    settings.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/endpoints
    contrib/ydb/public/sdk/cpp/src/library/time
)

END()
