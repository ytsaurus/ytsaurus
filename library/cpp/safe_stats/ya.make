LIBRARY()

SRCS(
    safe_stats.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/monlib/encode/json
    library/cpp/monlib/encode/spack
    library/cpp/nth_elements
)

END()

RECURSE(
    dynamic_counters
)
