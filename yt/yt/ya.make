RECURSE(
    benchmarks
    build
    client
    core
    experiments
    flow
    gpuagent
    library
    python
    scripts
    tools
    ytlib
)

IF (NOT OPENSOURCE)
    RECURSE(
        fuzz
        orm
        packages/tests_package
        pando
        utilities
    )
ENDIF()

IF (OS_LINUX)
    RECURSE(server)
ENDIF()

RECURSE(
    tests
)
