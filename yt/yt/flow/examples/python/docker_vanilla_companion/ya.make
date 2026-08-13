PY3_PROGRAM()

NO_CHECK_IMPORTS()

PY_SRCS(
    __init__.py
    __main__.py
    text_mapper.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
)

END()
