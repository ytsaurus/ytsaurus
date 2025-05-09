LIBRARY()

SRCS(
    async_expiring_cache.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
