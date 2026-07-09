LIBRARY()

SRCS(
    cache.cpp
)

END()

RECURSE_FOR_TESTS(
    unittests
    benchmarks
)
