PY23_LIBRARY()

OWNER(g:yt g:yt-python)

PEERDIR(
    yt/python/yt

    contrib/python/six
)

PY_SRCS(
    NAMESPACE yt.scheduler_tools

    __init__.py
    scheduler_utilization.py
)

END()
