LIBRARY()

SRCS(
    pipeline_init.cpp
)

PEERDIR(
    yt/yt/flow/lib/client
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
