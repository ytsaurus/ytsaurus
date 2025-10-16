PY3_LIBRARY()

PY_SRCS(
    __init__.py
    adapter.py
    constants.py
    data.py
    enum.py
    executor.py
    parser.py
    processor.py
    suite.py
)

PEERDIR(
    yt/python/yt/yson
    contrib/libs/sqlite3
    contrib/python/sqlglot
)

STYLE_PYTHON()

END()

RECURSE(test)
