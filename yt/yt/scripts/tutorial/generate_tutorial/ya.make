PY3_PROGRAM()

PY_SRCS(
    MAIN generate_tutorial.py
)

RESOURCE_FILES(
    PREFIX yt/yt/scripts/tutorial/generate_tutorial/scripts/
    PREFIX yt/yt/scripts/tutorial/generate_tutorial/data/
)


PEERDIR(
    yt/python/yt/wrapper
    yt/python/client
    contrib/python/zstandard
)

END()
