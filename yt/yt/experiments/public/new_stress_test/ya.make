PY3_PROGRAM()

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
    # yt/yt/python/yt_driver_bindings
    yt/yt/experiments/public/new_stress_test/lib
)

PY_SRCS(
    __main__.py
)

END()

RECURSE(
    odin_iterative_runner
)
