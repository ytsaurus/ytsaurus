LIBRARY()

SRCS(
    builder.cpp
    escape.cpp
    tskv_map.cpp
)

END()

RECURSE_FOR_TESTS(
    benchmark
    fuzz
    ut
)
