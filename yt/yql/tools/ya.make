RECURSE(
    fmrrun
    mrjob
    ytrun
)

IF (NOT OPENSOURCE)
    RECURSE(
        dq
        dqrun
        dqrun_light
        qt_stress
        query_replay
        query_replay_admin
        udf_admin
        ytfilerun
        ytflowrun
        ytflow_worker
        qtworker
        qtworker/full
    )
ENDIF()
