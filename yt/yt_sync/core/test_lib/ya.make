PY3_LIBRARY()

STYLE_PYTHON()


PY_SRCS(
    __init__.py
    client_faulty.py
    client_mock.py
    client_factory.py
    table_settings_builder.py
    yt_errors.py
)

PEERDIR(
    library/python/confmerge
    yt/yt_sync/core
    yt/yt_sync/core/spec
    yt/python/client
)

END()
