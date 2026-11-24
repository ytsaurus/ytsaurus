PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(
        yt/python_py2/yt/wrapper
    )
ELSE()
    PEERDIR(
        yt/python/yt/wrapper
    )
ENDIF()

PEERDIR_FROM_POLICY(
    ALLOW
    library/python/bstr
    library/python/yt
    library/python/yt/example
)

END()

