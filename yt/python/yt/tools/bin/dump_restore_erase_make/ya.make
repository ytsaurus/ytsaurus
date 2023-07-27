
PY2_PROGRAM(yt_dump_restore_erase)

OWNER(g:yt g:yt-python)

PEERDIR(
    yt/python/yt/tools
    yt/python/yt/wrapper
)

COPY_FILE(yt/python/yt/tools/bin/yt_dump_restore_erase.py __main__.py)

PY_SRCS(
    __main__.py
)

END()
