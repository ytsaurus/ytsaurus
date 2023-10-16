PY3_PROGRAM(yt_local)

PEERDIR(
    yt/python/yt/local
    yt/python/yt/environment/bin/yt_env_watcher_entry_point
)

COPY_FILE(yt/python/yt/local/bin/yt_local __main__.py)

PY_SRCS(__main__.py)

END()
