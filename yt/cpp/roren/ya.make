RECURSE(
    examples
    yt
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmarks
        bigrt
        bigrt/ut
        flow
        doc
        interface
        interface/ut
        local
        local/ut
        library
        transforms
        transforms/ut
        yt/test_medium
        yt/ut
    )

    RECURSE_FOR_TESTS(
        flow/extensions/bigrt/test_medium
        flow/extensions/logbroker/test_medium
    )
ENDIF()
