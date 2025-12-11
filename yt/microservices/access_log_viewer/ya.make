RECURSE(
    preprocessing
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
