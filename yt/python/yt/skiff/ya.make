PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/skiff)
ELSE()
    PEERDIR(
        yt/python/yt/yson

        contrib/python/six
    )

    PY_SRCS(
        NAMESPACE yt.skiff

        __init__.py
    )
ENDIF()

END()
