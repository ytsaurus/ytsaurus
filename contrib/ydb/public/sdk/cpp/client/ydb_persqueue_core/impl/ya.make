LIBRARY()

SRCS(
    read_session_messages.cpp
    common.cpp
    write_session.cpp
    write_session_impl.cpp
    read_session.h
    read_session.cpp
    persqueue.cpp
    persqueue_impl.cpp
)

PEERDIR(
    library/cpp/containers/disjoint_interval_tree
    library/cpp/grpc/client
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    contrib/ydb/library/persqueue/obfuscate
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/api/grpc/draft
)

END()
