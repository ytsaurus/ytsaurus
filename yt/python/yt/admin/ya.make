PY3_LIBRARY()

PEERDIR(
    yt/python/yt/admin/helpers
    yt/python/yt/admin/metrics
    yt/python/yt/wrapper
)

PY_SRCS(
    NAMESPACE yt.admin

    __init__.py
    _experimental.py
    describe.py
    logs_k8s.py
)

END()

RECURSE(
    helpers
    metrics
)

RECURSE_FOR_TESTS(
    tests
)
