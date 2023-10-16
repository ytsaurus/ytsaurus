PY3_PROGRAM(init_query_tracker_state)

PEERDIR(
    yt/python/yt/environment
)

COPY_FILE(yt/python/yt/environment/init_query_tracker_state.py __main__.py)

PY_SRCS(__main__.py)

END()
