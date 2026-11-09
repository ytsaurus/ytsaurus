LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

PEERDIR(
    yt/yt/flow/library/cpp/worker/no_engine
    yt/yt/library/query/engine
)

END()

RECURSE(
    no_engine
)

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        benchmarks
    )
ENDIF()
