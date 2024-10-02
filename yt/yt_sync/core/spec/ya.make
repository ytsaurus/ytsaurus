PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    consumer.py
    details.py
    node.py
    table.py
)

PEERDIR(
    contrib/python/dacite
    yt/python/yt/yson
)

END()

RECURSE_FOR_TESTS(
    ut
)
