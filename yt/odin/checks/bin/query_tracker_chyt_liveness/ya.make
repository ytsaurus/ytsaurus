PY3_PROGRAM(query_tracker_chyt_liveness)

PEERDIR(
    yt/odin/checks/lib/check_runner
    yt/odin/checks/lib/query_tracker_engine_liveness
)

PY_SRCS(
    __main__.py
)

END()
