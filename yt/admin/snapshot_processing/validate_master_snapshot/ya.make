PY23_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
    yt/admin/snapshot_processing/helpers
    contrib/python/six
)

PY_SRCS(
    validate_snapshot.py
)

END()
