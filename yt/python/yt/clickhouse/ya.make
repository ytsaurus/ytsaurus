PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/clickhouse)
ELSE()
    PEERDIR(
        yt/python/yt/wrapper
    )

    PY_SRCS(
        NAMESPACE yt.clickhouse

        __init__.py
        clickhouse.py
        client.py
        compatibility.py
        defaults.py
        execute.py
        helpers.py
        log_tailer.py
        spec_builder.py
        test_helpers.py
    )
ENDIF()

END()
