LIBRARY()

SRCS(
    block_range_map.cpp
    block_range.cpp
    thread_checker.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/storage/core/libs/coroutine
    library/cpp/lwtrace
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)
