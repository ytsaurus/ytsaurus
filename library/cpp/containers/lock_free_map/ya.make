LIBRARY()

SRCS(
    accessor.cpp
    backoff.cpp
    harris_michael_hashtable.cpp
    harris_michael_list.cpp
    key_select.cpp
    options.cpp
    reclaimer.cpp
    size_tracker.cpp
    static_vector.cpp
)

PEERDIR(
    library/cpp/threading/hazard_pointer
)

END()

RECURSE_FOR_TESTS(
    benchmark
    stress_test
    ut
)
