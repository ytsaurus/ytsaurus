PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/ypath)
ELSE()
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
ENDIF()

END()
