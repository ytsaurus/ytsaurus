RECURSE(
    examples
    yt
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmarks
        bigrt
        bigrt/ut
        doc
        interface
        interface/ut
        local
        local/tests/ut
        library
        transforms
        transforms/ut
        yt/test-medium
        yt/ut
    )
ENDIF()
