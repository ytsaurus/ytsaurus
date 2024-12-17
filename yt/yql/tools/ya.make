RECURSE(
    mrjob
    ytrun
)

IF (NOT OPENSOURCE)
    RECURSE(
    )
ENDIF()
