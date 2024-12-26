RECURSE(
    common
    client
    examples/tutorial
    http
    interface
    interface/logging
    io
    library
    raw_client
    rpc_client
    skiff
    util
    tests
)

IF (NOT OPENSOURCE)
    RECURSE(
        doc
        examples
        initialize
        tools
    )
ENDIF()

