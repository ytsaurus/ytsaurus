LIBRARY()

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h)

SRCS(
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/federated_topic/impl
    contrib/ydb/public/sdk/cpp/src/client/federated_topic/ut/fds_mock
)

END()

RECURSE_FOR_TESTS(
    ut
)
