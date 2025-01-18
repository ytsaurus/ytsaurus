PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    dry.py
    factory.py
    mock.py
    trace.py
    proxy.py
)

PEERDIR(
    yt/python/client_with_rpc
)

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        ft
    )
ENDIF()
