RECURSE(
    common
    common/ut
    client
    examples/tutorial
    http
    http/ut
    interface
    interface/ut
    interface/logging
    interface/logging/ut
    io
    library/parallel_io
    raw_client
    raw_client/ut
    skiff
    util
    util/ut
)

IF (NOT OPENSOURCE)
    RECURSE(
        doc
        examples
        initialize
        io/ut
        library
        tests
        tests_core_http
        tools
    )
ENDIF()

