LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    GLOBAL codecs.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/kafka
    contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic
)

END()
