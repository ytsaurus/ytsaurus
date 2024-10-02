PY3_PROGRAM(example-local)

PEERDIR(
    yt/yt/orm/example/python/local

    yt/yt/orm/python/orm/cli/local

    yt/python/yt/environment/bin/yt_env_watcher_entry_point
)

PY_SRCS(
    __main__.py
)

END()
