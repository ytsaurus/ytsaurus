PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/environment/api)
ELSE()
    PEERDIR(
        contrib/python/attrs
        yt/python/yt
    )

    PY_SRCS(
        NAMESPACE yt.environment.api

        __init__.py
    )
ENDIF()

END()
