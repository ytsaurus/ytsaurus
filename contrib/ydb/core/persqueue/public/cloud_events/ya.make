LIBRARY()

SRCS(
    actor.cpp
    actor.h
    cloud_events.h
    events_writer.cpp
    events_writer.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/json
    library/cpp/yt/coding
    library/cpp/unified_agent_client
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public/cloud_events/proto
    contrib/ydb/core/protos/schemeshard
    contrib/ydb/core/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)