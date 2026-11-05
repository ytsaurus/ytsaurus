PY23_LIBRARY()

PY_SRCS(
    test_utils.py
)

PEERDIR(
    contrib/python/six
    library/python/cyson
    yt/python/client
    yt/python/yt/wrapper
    yt/python/yt/environment

    yql/essentials/providers/common/proto
)

END()
