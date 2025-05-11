LIBRARY()

SRCS(
    managed_cache_listener.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/threading/cancellation
)

END()

RECURSE_FOR_TESTS(
    ut
)
