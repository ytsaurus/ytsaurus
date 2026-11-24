PY3_LIBRARY()

PEERDIR(
    yt/python/yt
)

PY_SRCS(
    __init__.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/library/peerdir_policy.inc)

END()
