LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h)

SRCS(
    contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/topic
    contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl
)

END()
