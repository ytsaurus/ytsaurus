PY3_LIBRARY()

PY_SRCS(
    ytprof.py
)

PY_NAMESPACE(helpers)

PEERDIR(
    yt/yt/library/ytprof/proto
)

END()
