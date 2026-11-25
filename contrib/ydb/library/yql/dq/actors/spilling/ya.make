YQL_LIBRARY()

SRCS(
    channel_storage_actor.cpp
    channel_storage.cpp
    compute_storage_actor.cpp
    compute_storage.cpp
    spilling_file.cpp
    spilling.cpp
)

PEERDIR(
    library/cpp/monlib/service/pages
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util
    contrib/ydb/library/services
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/actors/spilling/counters
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/dq/runtime
    yql/essentials/utils
)

END()

RECURSE(
    counters
)

RECURSE_FOR_TESTS(
    ut
)
