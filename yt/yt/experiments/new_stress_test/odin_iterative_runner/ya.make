PY3_PROGRAM()

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
    yt/yt/experiments/new_stress_test/lib
    yt/yt/experiments/new_stress_test/check_runner
)

FILES(
    config.yson
)

PY_SRCS(
    __main__.py
)

END()
