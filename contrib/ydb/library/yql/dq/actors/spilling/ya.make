YQL_LIBRARY()

SRCS(
    channel_storage_actor.cpp
    channel_storage.cpp
    compute_storage_actor.cpp
    compute_storage.cpp
    spilling_counters.cpp
    spilling_file.cpp
    spilling.cpp
)

PEERDIR(
    contrib/ydb/library/services
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/runtime
    yql/essentials/utils

    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service/pages
)

END()

RECURSE_FOR_TESTS(
    ut
)
