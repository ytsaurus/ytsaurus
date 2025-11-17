RECURSE(
    lib
    yt
)


IF (NOT OPENSOURCE)
    RECURSE(
        bigrt
    )
ENDIF()
