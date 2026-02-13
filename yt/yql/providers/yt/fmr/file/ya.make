RECURSE(
    metadata
    upload
)

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
