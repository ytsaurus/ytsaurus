RECURSE(
    client
    core
)

IF (NOT OPENSOURCE)
    RECURSE(
        flow
    )
ENDIF()
