PY3_PROGRAM()

PEERDIR(
    yt/python/client
    contrib/python/psutil
)

PY_SRCS(__main__.py)

IF (OPENSOURCE)
    NO_CHECK_IMPORTS()
ENDIF()

END()

IF (NOT OPENSOURCE)
    RECURSE(
        add_column
        vh3
        datalens
    )
ENDIF()
