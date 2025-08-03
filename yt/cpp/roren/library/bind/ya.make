LIBRARY()

SRCS(
    bind.cpp
)

PEERDIR(
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)
