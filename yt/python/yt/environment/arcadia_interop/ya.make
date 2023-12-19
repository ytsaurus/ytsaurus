PY23_LIBRARY()

PEERDIR(
    yt/python/yt
)

PY_SRCS(
    NAMESPACE yt.environment.arcadia_interop

    __init__.py
    arcadia_interop.py
)

END()
