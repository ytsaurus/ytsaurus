LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

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
