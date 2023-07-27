PY23_LIBRARY()

OWNER(levysotsky)

PY_SRCS(
    NAMESPACE yt.infra_api
    __init__.py
    api.py
)

IF(PYTHON2)
    PEERDIR(
        contrib/deprecated/python/typing
    )
ENDIF()

END()
