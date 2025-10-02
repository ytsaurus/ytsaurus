PY3_PROGRAM()

PY_SRCS(
    MAIN generate_tutorial.py
)
RESOURCE_FILES(
  PREFIX scripts/
  PREFIX data/
)


PEERDIR(
    yt/python/yt/wrapper
    yt/python/client
    contrib/python/zstandard
)

END()
