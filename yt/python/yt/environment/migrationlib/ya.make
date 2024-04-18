PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/environment/migrationlib)
ELSE()
    PEERDIR(
        yt/python/yt
        yt/python/yt/yson
        yt/python/yt/wrapper
    )

    PY_SRCS(
        NAMESPACE yt.environment.migrationlib
        __init__.py
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    tests
)
