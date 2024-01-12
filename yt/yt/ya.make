RECURSE(
    build
    benchmarks
    client
    core
    experiments
    library
    python
    tools
    ytlib
)

IF (NOT OPENSOURCE)
    RECURSE(
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
