IF (NOT OPENSOURCE)
    RECURSE(
        chyt
    )
ENDIF()

RECURSE(
    cli
    helpers
    integration
    unittest
)
