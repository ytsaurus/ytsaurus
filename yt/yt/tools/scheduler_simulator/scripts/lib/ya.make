PY3_LIBRARY()

OWNER(g:yt)

PY_SRCS(
    NAMESPACE yt.tools.scheduler_simulator.scripts.lib

    __init__.py
)

PEERDIR(
    yt/python/client
)

END()
