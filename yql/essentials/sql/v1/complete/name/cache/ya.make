LIBRARY()

SRCS(
    cache.cpp
    cached.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()

RECURSE(
    local
)
