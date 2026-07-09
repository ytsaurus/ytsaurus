G_BENCHMARK()

SRCS(
    cache_bench.cpp
)

PEERDIR(
    library/cpp/containers/concurrent_hash
    library/cpp/containers/insert_only_concurrent_cache
    library/cpp/containers/lock_free_map
)

SIZE(MEDIUM)

END()
