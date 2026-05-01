LIBRARY()

SRCS(
    public.cpp
    trace_helpers.cpp
    vchunk_counters.cpp
    vhost_stats_simple.cpp
    vhost_stats_test.cpp
    vhost_stats.cpp
    volume_counters.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/service
    contrib/ydb/core/nbs/cloud/storage/core/libs/diagnostics
    contrib/ydb/core/nbs/cloud/storage/core/protos

    contrib/ydb/library/actors/wilson

    util
)

END()
