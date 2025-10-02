PY3_PROGRAM()

PEERDIR(
    contrib/python/click
    contrib/python/Jinja2/py3
)

PY_SRCS(
    NAMESPACE yt_record_render
    __init__.py
    __main__.py
)

END()
