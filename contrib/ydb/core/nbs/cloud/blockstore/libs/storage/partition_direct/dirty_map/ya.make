LIBRARY()

GENERATE_ENUM_SERIALIZATION(dirty_map.h)
GENERATE_ENUM_SERIALIZATION(inflight_info.h)

SRCS(
    dirty_map.cpp
    inflight_info.cpp
    range_locker.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/common
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
