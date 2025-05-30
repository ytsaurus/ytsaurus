LIBRARY()

SRCS(
    cache.cpp
    cached.cpp
    size.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()

RECURSE(
    local
)

RECURSE_FOR_TESTS(
    ut
)
