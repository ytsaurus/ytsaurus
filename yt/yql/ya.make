IF (NOT EXPORT_CMAKE)

RECURSE(
    agent
    plugin
    providers
    tools
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
