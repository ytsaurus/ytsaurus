IF (NOT EXPORT_CMAKE)

RECURSE(
    agent
    plugin
    providers
    tools
    dq_vanilla_job
    dq_vanilla_job.lite
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
