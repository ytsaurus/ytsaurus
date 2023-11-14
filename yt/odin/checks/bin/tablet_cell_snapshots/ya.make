PY3_PROGRAM(tablet_cell_snapshots)

PEERDIR(
    yt/odin/checks/lib/tablet_cell_helpers
    yt/odin/checks/lib/check_runner
    yt/python/yt/yson
    yt/python/yt
)

PY_SRCS(
    __main__.py
)

END()
