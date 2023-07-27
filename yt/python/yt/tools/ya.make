PY23_LIBRARY()

OWNER(g:yt g:yt-python)

PEERDIR(
    yt/python/yt

    contrib/python/six
)

PY_SRCS(
    NAMESPACE yt.tools

    __init__.py
    atomic.py
    dump_restore_client.py
    dynamic_tables.py
)

END()
