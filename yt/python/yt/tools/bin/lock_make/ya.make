
PY2_PROGRAM(yt_lock)

OWNER(g:yt g:yt-python)

PEERDIR(yt/python/yt/wrapper)

COPY_FILE(yt/python/yt/tools/bin/lock.py __main__.py)

PY_SRCS(
    __main__.py
)

END()
