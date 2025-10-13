RECURSE(
    preprocessing
    http_service
)

IF (NOT OPENSOURCE)
    RECURSE(
        bin
        lib
    )

    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()