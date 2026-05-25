LIBRARY()

SRCS(
    actor.cpp
    common_app.cpp
    heartbeat.cpp
    key.cpp
    microseconds_sliding_window.cpp
    partition_id.cpp
    percentiles.cpp
    partitioning_keys_manager.cpp
)

GENERATE_ENUM_SERIALIZATION(sourceid_info.h)

PEERDIR(
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/persqueue/public
    contrib/ydb/core/persqueue/public/partition_key_range
    contrib/ydb/library/actors/core
    contrib/ydb/library/logger
    contrib/ydb/library/kll_median
)

END()

RECURSE(
    proxy
)

RECURSE_FOR_TESTS(
    ut
)
