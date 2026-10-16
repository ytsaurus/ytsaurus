RECURSE(
    lib
    pipeline
    trunk
)

IF (NOT OPENSOURCE)
    RECURSE(
        25_4
        26_1
        ytflow_latest
    )
ENDIF()
