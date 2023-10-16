LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    chunk_pool.cpp
    chunk_slice_fetcher.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/server/lib/chunk_pools
)

END()
