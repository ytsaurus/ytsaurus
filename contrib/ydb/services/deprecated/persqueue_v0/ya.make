LIBRARY()

SRCS(
    grpc_pq_clusters_updater_actor.cpp
    grpc_pq_read.cpp
    grpc_pq_read_actor.cpp
    grpc_pq_write.cpp
    grpc_pq_write_actor.cpp
    persqueue.cpp
)

PEERDIR(
    contrib/ydb/services/deprecated/persqueue_v0/api/grpc
    contrib/ydb/services/deprecated/persqueue_v0/api/protos
    contrib/ydb/library/persqueue/deprecated/read_batch_converter
    contrib/ydb/core/base
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/client/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/mind/address_classification
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public/counters
    contrib/ydb/core/persqueue/writer
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
    contrib/ydb/library/persqueue/topic_parser
    contrib/ydb/services/lib/actors
    contrib/ydb/services/lib/sharding
    contrib/ydb/services/persqueue_v1
    contrib/ydb/services/metadata
)

END()
