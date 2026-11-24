LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    settings.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/common
    contrib/libs/ydb-cpp-sdk/src/client/types/credentials
)

END()
