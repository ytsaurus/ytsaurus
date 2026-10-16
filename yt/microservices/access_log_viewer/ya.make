RECURSE(
    http_service
    preprocessing
    raw_access_log_preprocessing
)

IF (NOT OPENSOURCE)
    RECURSE(
        bin
        lib
        raw_log_preprocessing
    )

    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
