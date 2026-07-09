PY3_PROGRAM(pipeline)

NO_CHECK_IMPORTS()

PY_SRCS(
    __init__.py
    __main__.py
    visit_tester.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
)

END()
