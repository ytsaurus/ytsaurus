RECURSE(
    mrjob
    ytrun
)

IF (NOT OPENSOURCE)
    RECURSE(
        ytflowrun
    )
ENDIF()
