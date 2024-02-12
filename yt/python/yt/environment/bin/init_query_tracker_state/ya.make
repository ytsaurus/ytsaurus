PY3_PROGRAM(init_query_tracker_state)

PEERDIR(
    yt/python/yt/environment
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
)

COPY_FILE(yt/python/yt/environment/init_query_tracker_state.py __main__.py)

PY_SRCS(__main__.py)

END()
