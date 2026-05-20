LIBRARY()

GENERATE_ENUM_SERIALIZATION(host_state.h)

SRCS(
    host_stat.cpp
    host_state.cpp
    oracle.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/common
    contrib/ydb/core/nbs/cloud/blockstore/config
)

END()

RECURSE_FOR_TESTS(
    ut
)
