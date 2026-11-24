LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    federated_read_session.h
    federated_read_session.cpp
    federated_read_session_event.cpp
    federated_write_session.h
    federated_write_session.cpp
    federated_topic_impl.h
    federated_topic_impl.cpp
    federated_topic.cpp
    federation_observer.h
    federation_observer.cpp
    federated_deferred_commit.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    contrib/libs/ydb-cpp-sdk/src/library/persqueue/obfuscate
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/grpc
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/topic/impl
    contrib/libs/ydb-cpp-sdk/src/client/proto
)

END()
