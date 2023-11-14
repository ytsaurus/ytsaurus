RECURSE(
    bin
    lib
    tests
    scripts
)
    
IF (NOT OPENSOURCE)
    RECURSE(
        config
    )
ENDIF()
