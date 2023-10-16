PY3_PROGRAM(yt_env_watcher)

PEERDIR(yt/python/yt)

COPY_FILE(yt/python/yt/environment/bin/yt_env_watcher __main__.py)

PY_SRCS(__main__.py)

END()
