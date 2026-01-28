PY23_LIBRARY()

PY_SRCS(
    NAMESPACE yt.admin
    __init__.py
    fetch_cluster_info.py
    fetch_cluster_logs.py
)

PEERDIR(
    yt/python/yt/wrapper
    contrib/python/python-dateutil
)

END()