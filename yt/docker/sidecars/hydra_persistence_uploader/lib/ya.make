PY3_LIBRARY(hydra_persistence_uploader)

PY_SRCS(
    NAMESPACE hydra_persistence_uploader
    __init__.py
)

PEERDIR(
    contrib/python/attrs
    contrib/python/requests
    contrib/python/APScheduler
)

END()
