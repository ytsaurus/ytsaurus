LIBRARY()

SRCS(
    host_stat.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
