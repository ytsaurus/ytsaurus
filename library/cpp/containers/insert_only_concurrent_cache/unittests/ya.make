GTEST()

SRCS(
    cache_ut.cpp
)

PEERDIR(
    library/cpp/containers/concurrent_hash
    library/cpp/containers/insert_only_concurrent_cache
)

SIZE(SMALL)

END()
