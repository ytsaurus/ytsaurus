PY3_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
    yt/yt/experiments/public/new_stress_test/lib
)

NO_LINT()

PY_SRCS(
    __init__.py
)

END()
