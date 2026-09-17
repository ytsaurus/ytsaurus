IF (NOT EXPORT_CMAKE)

RECURSE(
    agent
    library
    plugin
    providers
    tools
    dq_vanilla_job
    dq_vanilla_job.lite
    scripts
    udfs
)

RECURSE_FOR_TESTS(
    tests
)

IF (NOT OPENSOURCE)
    RECURSE(
        purecalc
    )
ENDIF()

ENDIF()
