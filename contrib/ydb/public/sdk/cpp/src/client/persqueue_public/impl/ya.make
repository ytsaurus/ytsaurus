LIBRARY()

SRCS(
    aliases.h
    common.h
    common.cpp
    persqueue_impl.h
    persqueue_impl.cpp
    persqueue.cpp
    read_session.h
    read_session.cpp
    read_session_messages.cpp
    write_session_impl.h
    write_session_impl.cpp
    write_session.h
    write_session.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    library/cpp/containers/disjoint_interval_tree
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic/codecs
    contrib/ydb/public/sdk/cpp/src/client/topic/common
    contrib/ydb/public/sdk/cpp/src/client/topic/impl
    contrib/ydb/public/sdk/cpp/src/client/scheme

)

END()
