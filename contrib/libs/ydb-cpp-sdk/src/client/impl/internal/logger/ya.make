LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    log.cpp
)

PEERDIR(
    library/cpp/logger
)

END()
