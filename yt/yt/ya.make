RECURSE(
    benchmarks
    build
    client
    core
    experiments
    gpuagent
    library
    python
    scripts
    tools
    ytlib
)

IF (NOT OPENSOURCE)
    RECURSE(
        flow
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
