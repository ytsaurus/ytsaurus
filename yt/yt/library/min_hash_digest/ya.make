LIBRARY()

SRCS(
    min_hash_digest.cpp
)

PEERDIR(
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
