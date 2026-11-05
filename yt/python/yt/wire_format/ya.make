PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/wire_format)
ELSE()
    PEERDIR(
        yt/python/yt/yson
    )

    PY_SRCS(
        NAMESPACE yt.wire_format

        __init__.py
        wire_format.py
    )
ENDIF()

END()
