RECURSE(
    common
)

RECURSE_FOR_TESTS(
    agent
    core_ut
    s-expressions
    sql
)

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        dq
    )
ENDIF()
