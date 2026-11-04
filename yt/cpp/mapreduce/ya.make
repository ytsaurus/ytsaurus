RECURSE(
    common
    client
    doc
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
        examples
        initialize
        tools
    )
ENDIF()
