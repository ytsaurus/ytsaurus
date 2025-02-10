LIBRARY()

SRCS(
    bitset.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
    benchmark
)
