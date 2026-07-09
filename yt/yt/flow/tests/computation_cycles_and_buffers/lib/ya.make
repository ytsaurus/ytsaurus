PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    test_base.py
    yt_sync.py
)

PEERDIR(
    yt/yt/flow/library/python/queue
)

END()
