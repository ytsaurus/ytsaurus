PY3_PROGRAM(init_queue_agent_state)

PEERDIR(
    yt/python/yt/environment
)

COPY_FILE(yt/python/yt/environment/init_queue_agent_state.py __main__.py)

PY_SRCS(
    __main__.py
)

END()

