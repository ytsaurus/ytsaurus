LIBRARY()

SRCS(
    grpc_pq_read.cpp
    grpc_pq_read.h
    grpc_pq_schema.cpp
    grpc_pq_schema.h
    grpc_pq_write.cpp
    grpc_pq_write.h
    persqueue.cpp
    persqueue.h
    services_initializer.cpp
    topic.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/containers/disjoint_interval_tree
    contrib/ydb/library/grpc/server
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services
    contrib/ydb/core/kqp
    contrib/ydb/core/persqueue
    contrib/ydb/core/persqueue/codecs
    contrib/ydb/core/persqueue/writer
    contrib/ydb/core/protos
    contrib/ydb/core/ydb_convert
    contrib/ydb/library/aclib
    contrib/ydb/public/sdk/cpp/src/library/persqueue/obfuscate
#    ydb/library/persqueue/tests
    contrib/ydb/library/persqueue/topic_parser
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/services/lib/actors
    contrib/ydb/services/lib/sharding
    contrib/ydb/services/persqueue_v1/actors
)

END()

RECURSE(
    actors
)

RECURSE_FOR_TESTS(
    ut
    ut/new_schemecache_ut
    ut/describes_ut
)
