
PY23_LIBRARY()

OWNER(
    g:yt g:yt-python
)

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
    contrib/python/simplejson
)

PY_SRCS(
    # TODO(asaitgalin): Remove yt prefix
    NAMESPACE yt.transfer_manager.client

    __init__.py
    client.py
    global_client.py
)

END()
