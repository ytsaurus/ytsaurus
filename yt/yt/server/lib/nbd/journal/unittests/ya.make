GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    block_flusher_ut.cpp
    block_map_ut.cpp
    dirty_block_pool_ut.cpp
)

PEERDIR(
    yt/yt/server/lib/nbd/journal
    yt/yt/core/test_framework
)

END()
