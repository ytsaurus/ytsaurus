RECURSE(
    yt
)

IF (NOT OPENSOURCE)
    RECURSE(
        ytflow
    )
ENDIF()
