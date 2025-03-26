RECURSE(
    bin
    lib
    tests
)

IF (NOT OPENSOURCE)
    RECURSE(
        config
        scripts
    )
ENDIF()
