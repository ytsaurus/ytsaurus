OWNER(
    exprmntr
)

LIBRARY()

BUILD_ONLY_IF(OS_LINUX)

PY_SRCS(
    __init__.py
    yt_stuff.py
)

PEERDIR(
    contrib/python/yt
    devtools/swag/lib
)

END()

