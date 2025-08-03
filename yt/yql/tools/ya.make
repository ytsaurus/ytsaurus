RECURSE(
    fmrrun
    mrjob
    ytrun
)

IF (NOT OPENSOURCE)
    RECURSE(
        ytfilerun
        ytflowrun
    )
ENDIF()
