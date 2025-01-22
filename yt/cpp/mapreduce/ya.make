RECURSE(
    common
    client
    examples/tutorial
    http
    http_client
    interface
    interface/logging
    io
    library
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

