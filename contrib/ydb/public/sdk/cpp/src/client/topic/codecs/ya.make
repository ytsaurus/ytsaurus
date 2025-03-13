LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    codecs.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
)

END()
