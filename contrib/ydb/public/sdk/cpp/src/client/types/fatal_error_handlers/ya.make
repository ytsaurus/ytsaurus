LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    handlers.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
