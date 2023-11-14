PY3_LIBRARY()

PY_SRCS(
    NAMESPACE yt_odin_checks.lib.tablet_cells

    __init__.py
)

PEERDIR(
    yt/odin/checks/lib/tablet_cell_helpers
    yt/python/yt/yson
    yt/python/yt

    contrib/python/tabulate
)

END()
