PY23_LIBRARY()

IF (PYTHON2)
    IF (NOT OPENSOURCE)
        PEERDIR(yt/python_py2/yt/environment/migrationlib)
    ENDIF()
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

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "yt")
    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
