PY2_LIBRARY()

OWNER(g:yt g:yt-python)

PEERDIR(
    contrib/python/Flask
)

PY_SRCS(
    NAMESPACE yt.flask_helpers

    __init__.py
)

END()
