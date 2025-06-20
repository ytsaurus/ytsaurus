PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    NAMESPACE yt.data.pandas

    __init__.py
    _schema.py
    _arrow.py
)

PEERDIR(
    yt/python/yt/wrapper
    contrib/python/pyarrow
    contrib/python/pandas
)

END()

RECURSE_FOR_TESTS(tests)
