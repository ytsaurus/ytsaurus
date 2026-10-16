PY3_LIBRARY()

PY_SRCS(
    __init__.py
    flow_execute_test_base.py
    yt_sync.py
)

PEERDIR(
    contrib/python/requests
    library/python/testing/yatest_common
    yt/yt/flow/library/python/bullied_process
    yt/yt/flow/library/python/queue
    yt/python/yt/wrapper
)

END()
