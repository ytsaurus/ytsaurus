RECURSE(
    sql2yql
    yt
    yt_file
)

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ydb")
    RECURSE(
        dq_file
        hybrid_file
    )
ENDIF()

IF (NOT OPENSOURCE)
    RECURSE(
        runners
        ytflow
    )
ENDIF()
