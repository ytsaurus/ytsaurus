LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    settings.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/common
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

END()
