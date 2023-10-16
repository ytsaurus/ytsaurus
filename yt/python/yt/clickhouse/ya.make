PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

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

END()
