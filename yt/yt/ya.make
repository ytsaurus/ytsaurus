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
    orm
)

IF (NOT OPENSOURCE)
    RECURSE(
        flow
        fuzz
        packages/tests_package
        utilities
        scripts
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
