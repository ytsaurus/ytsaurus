PY3_PROGRAM(yt_local)

PEERDIR(
    yt/python/yt/local
    yt/python/yt/environment/bin/yt_env_watcher_entry_point
    yt/python/yt/environment/components/query_tracker
    yt/python/yt/environment/components/yql_agent

    yt/yt/python/yt_driver_bindings
)

COPY_FILE(yt/python/yt/local/bin/yt_local __main__.py)

PY_SRCS(__main__.py)

END()
