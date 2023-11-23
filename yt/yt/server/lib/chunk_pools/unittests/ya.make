GTEST(unittester-chunk-pools)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

IF (AUTOCHECK)
    ENV(SKIP_PORTO_TESTS=1)
ENDIF()

IF (DISTBUILD) # TODO(prime@): this is always on
    ENV(SKIP_PORTO_TESTS=1)
ENDIF()

SRCS(
    chunk_pools_helpers.cpp
    input_chunk_mapping_ut.cpp
    job_size_adjuster_ut.cpp
    job_splitting_base_ut.cpp
    multi_chunk_pool_ut.cpp
    ordered_chunk_pool_ut.cpp
    sorted_chunk_pool_ut.cpp
    sorted_chunk_pool_new_keys_ut.cpp
    unordered_chunk_pool_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/server/lib/chunk_pools
    yt/yt/server/lib/chunk_pools/mock
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(MEDIUM)

END()
