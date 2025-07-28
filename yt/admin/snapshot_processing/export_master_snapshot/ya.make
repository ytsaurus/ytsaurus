PY3_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
    yt/admin/snapshot_processing/helpers
)

PY_SRCS(
    export_snapshot.py
)

END()
