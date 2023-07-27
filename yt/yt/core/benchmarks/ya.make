G_BENCHMARK(benchmarks-core)

OWNER(g:yt)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ENDIF()

IF (YT_TEAMCITY AND SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    atomic_ptr.cpp
    action_queue.cpp
    binary_search.cpp
    callback.cpp
    coroutine.cpp
    is_space.cpp
    guid.cpp
    mpmc_queue.cpp
    mpsc_queue.cpp
    packed_vector.cpp
    ref_counted_tracker.cpp
    timing.cpp
    thread_pool.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework

    library/cpp/yt/threading
)

REQUIREMENTS(
    ram:32
)

END()
