PY3_PROGRAM(reader)

PY_SRCS(
    MAIN main.py
)

PEERDIR(
    contrib/python/zstandard
    contrib/python/tqdm
)

END()
