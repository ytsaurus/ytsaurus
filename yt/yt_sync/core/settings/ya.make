PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
)

PEERDIR(
    yt/yt_sync/core/constants

    library/python/confmerge
)

END()

RECURSE_FOR_TESTS(
    ut
)
