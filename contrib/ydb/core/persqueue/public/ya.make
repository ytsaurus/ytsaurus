LIBRARY()

SRCS(
    config.cpp
    inflight_limiter.cpp
    pq_database.cpp
    pq_rl_helpers.cpp
    utils.cpp
    write_id.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/metering
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public/cloud_events
    contrib/ydb/core/protos
    contrib/ydb/core/tx/scheme_board
)

END()

RECURSE(
    cluster_tracker
    codecs
    counters
    describer
    fetcher
    list_topics
    mlp
    partition_index_generator
    partition_key_range
    schema
    write_meta
    cloud_events
)
