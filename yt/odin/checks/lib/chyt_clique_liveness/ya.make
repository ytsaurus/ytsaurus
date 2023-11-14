PY3_LIBRARY()

PY_SRCS(
    NAMESPACE yt_odin_checks.lib.chyt_clique_liveness

    __init__.py
)

PEERDIR(
    yt/python/contrib/python-requests
    yt/python/yt/wrapper
)

END()
