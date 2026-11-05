PY3_LIBRARY()

PEERDIR(
    yt/python/client_with_rpc
    yt/yt/tests/library/smooth_movement_helper
    contrib/python/tabulate
)

PY_SRCS(
    __init__.py
    common.py
    create_chaos.py
    create.py
    create_replicated.py
    disturbance.py
    inserter.py
    log.py
    reader_writer.py
    runner.py
    selecter.py
    setup.py
    smooth_move.py
)

END()
