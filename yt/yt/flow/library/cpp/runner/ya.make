LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

PEERDIR(
    yt/yt/flow/library/cpp/runner/no_engine
    yt/yt/flow/library/cpp/worker
)

END()

RECURSE(
    no_engine
)

RECURSE_FOR_TESTS(
    unittests
)
