RECURSE(
    client
    codegen
    example
    python
    server
)

IF (NOT OPENSOURCE)
    RECURSE(
        client
        codegen
        library
        go
        java/client
        library
        python
        tools
        server
        server/admin
        server/idm_provisor
        server/idm_provisor/bin
        server/program
        tools
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
    tests
)
