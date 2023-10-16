RECURSE(
    build
    client
    core
    library
    python
    tools
    ytlib
)

IF (OPENSOURCE)
    RECURSE(
        experiments/new_stress_test
    )
ENDIF()
    
IF (NOT OPENSOURCE)
    RECURSE(
        experiments
        fuzz
        orm
        packages/tests_package
        utilities
    )
ENDIF()

IF (OS_LINUX)
    RECURSE(server)
ENDIF()

IF (NOT SANITIZER_TYPE)
    RECURSE(
        tests
    )
ENDIF()
