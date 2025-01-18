PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    chaos_helpers.py
    collocations.py
    consumers.py
    helpers.py
    node_manager.py
    switcher.py
    unmanaged.py
)

PEERDIR(
    yt/yt_sync/action
    yt/yt_sync/core
    yt/yt_sync/core/diff

    yt/python/client
)

END()

RECURSE_FOR_TESTS(
    ut
)
