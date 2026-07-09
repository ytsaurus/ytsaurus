LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

# The process-function core (interfaces, runtime contexts, registration) now lives in common;
# this facade re-exports it so existing PEERDIRs keep resolving. The host adapters and the test
# helpers live in the subdirectories below.
PEERDIR(
    yt/yt/flow/library/cpp/common
)

END()

RECURSE(
    host
    testing
)

RECURSE_FOR_TESTS(
    unittest
)

IF (NOT SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        benchmarks
    )
ENDIF()
