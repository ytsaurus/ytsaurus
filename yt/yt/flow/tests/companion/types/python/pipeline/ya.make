PY3_PROGRAM(pipeline)

NO_CHECK_IMPORTS()

PY_SRCS(
    __init__.py
    __main__.py
    type_mapper.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
)

END()
