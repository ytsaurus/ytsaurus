LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    handlers.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/types/exceptions
)

END()
