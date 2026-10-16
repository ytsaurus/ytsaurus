PY3_PROGRAM()

NO_CHECK_IMPORTS()

PY_SRCS(
    __init__.py
    __main__.py
    compaction_mapper.py
    total_writer.py
)

PEERDIR(
    yt/yt/flow/library/python/companion
)

END()

RECURSE_FOR_TESTS(
    test
    unittests
)
