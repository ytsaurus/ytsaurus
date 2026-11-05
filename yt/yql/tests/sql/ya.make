RECURSE(
    dq_file
    hybrid_file
    runners
    yt_file
)

IF (NOT OPENSOURCE)
    RECURSE(
        sql2yql
        yt
        ytflow
    )
ENDIF()
