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
    library/user_job_statistics
    raw_client
    raw_client/ut
    skiff
    util
    util/ut
    tests
    tests_core_http
)

IF (NOT OPENSOURCE)
    RECURSE(
        doc
        examples
        initialize
        io/ut
        library
        tools
    )
ENDIF()

