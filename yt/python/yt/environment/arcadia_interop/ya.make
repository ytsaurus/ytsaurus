PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/environment/arcadia_interop)
ELSE()
    PEERDIR(
        yt/python/yt
    )

    PY_SRCS(
        NAMESPACE yt.environment.arcadia_interop

        __init__.py
        arcadia_interop.py
    )
ENDIF()

END()
