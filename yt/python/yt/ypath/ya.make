PY23_LIBRARY()

PEERDIR(
    yt/python/yt
    yt/python/yt/yson

    contrib/python/six
)

PY_SRCS(
    NAMESPACE yt.ypath

    common.py
    __init__.py
    parser.py
    rich.py
    tokenizer.py
)

END()
