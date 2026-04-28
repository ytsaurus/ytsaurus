PY3_LIBRARY()

PEERDIR(
    contrib/python/python-dateutil
    contrib/python/PyYAML
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
