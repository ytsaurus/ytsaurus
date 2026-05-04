RECURSE(
    fmrrun
    mrjob
    ytrun
)

IF (NOT OPENSOURCE)
    RECURSE(
        dq
        query_replay
        ytfilerun
        ytflowrun
        ytflow_worker
        qtworker
        qtworker/full
    )
ENDIF()
