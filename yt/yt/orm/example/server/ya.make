RECURSE(
    bin
    library
    proto
)

IF (NOT AUTOCHECK OR SANDBOXING OR SANITIZER_TYPE)
    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
