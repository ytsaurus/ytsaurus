RECURSE(
    build
    client
    codegen
    environment
    python
    server
)
    
IF (NOT OPENSOURCE)
    RECURSE(
        java
        tools
    )
ENDIF()

IF (NOT AUTOCHECK OR SANDBOXING OR SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        benchmarks
        tests
    )
ENDIF()
