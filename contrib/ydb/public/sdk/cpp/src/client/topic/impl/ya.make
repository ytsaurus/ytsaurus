LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    common.h
    common.cpp
    counters_logger.h
    deferred_commit.cpp
    event_handlers.cpp
    offsets_collector.cpp
    read_session_event.cpp
    read_session_impl.ipp
    read_session.h
    read_session.cpp
    topic_impl.h
    topic_impl.cpp
    topic.cpp
    transaction.cpp
    write_session_impl.h
    write_session_impl.cpp
    write_session.h
    write_session.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    contrib/ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic/common
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
    contrib/ydb/public/sdk/cpp/src/client/proto
)

END()
