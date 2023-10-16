IF (NOT GCC AND NOT OS_DARWIN)
    RECURSE(
        server
    )
ENDIF()

RECURSE(
    client
    trampoline
    controller
)
    
IF (NOT OPENSOURCE)
    RECURSE(
        packages
    )
ENDIF()

RECURSE_FOR_TESTS(
    tests
)
