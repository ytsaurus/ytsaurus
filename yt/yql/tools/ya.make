RECURSE(
    fmrrun
    mrjob
    ytrun
)

IF (NOT OPENSOURCE)
    RECURSE(
        dq
        dqrun
        qt_stress
        query_replay
        ytfilerun
        ytflowrun
        ytflow_worker
        qtworker
        qtworker/full
    )
ENDIF()
