LIBRARY()

SRCS(
    unordered_invoker.cpp
)

PEERDIR(
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)
