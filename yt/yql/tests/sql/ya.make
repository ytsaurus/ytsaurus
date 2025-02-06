RECURSE(
    sql2yql
    yt
    yt_file
)

IF (NOT OPENSOURCE)
    RECURSE(
        ytflow
    )
ENDIF()
