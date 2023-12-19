PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/python/yt/environment

    # It is necessary for possible presence of tables in YSON format in local cypress dir.
    yt/yt/python/yt_yson_bindings
    yt/yt/python/yt_driver_bindings

    contrib/python/six
)

PY_SRCS(
    NAMESPACE yt.local

    __init__.py
    commands.py
    helpers.py
)

END()
