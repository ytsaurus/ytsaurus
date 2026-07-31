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
        udf_admin
        ytfilerun
        ytflowrun
        ytflow_worker
        qtworker
        qtworker/full
    )
ENDIF()
