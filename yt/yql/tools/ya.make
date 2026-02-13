RECURSE(
    fmrrun
    mrjob
    ytrun
)

IF (NOT OPENSOURCE)
    RECURSE(
        query_replay
        ytfilerun
        ytflowrun
        ytflow_worker
    )
ENDIF()
