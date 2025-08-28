LIBRARY()

PEERDIR(
    library/cpp/json/common
)

SRCS(
    parser.rl6
    parser.cpp
    reader.cpp
)

END()

RECURSE(
    bench
)

RECURSE_FOR_TESTS(
    ut
)
