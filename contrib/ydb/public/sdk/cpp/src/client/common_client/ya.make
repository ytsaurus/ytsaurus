LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    settings.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/internal/common
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

END()
