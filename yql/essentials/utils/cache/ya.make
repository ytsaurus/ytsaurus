LIBRARY()

SRCS(
    managed_cache_listener.cpp
)

PEERDIR(
    library/cpp/threading/cancellation
    library/cpp/threading/future
    library/cpp/threading/task_scheduler
)

END()

RECURSE_FOR_TESTS(
    ut
)
