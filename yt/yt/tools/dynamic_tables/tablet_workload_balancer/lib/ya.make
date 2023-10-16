PY3_LIBRARY(tablet_balancer_lib)

PEERDIR(
    contrib/python/matplotlib
    contrib/python/numpy
    yt/python/yt/test_helpers
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
)

NO_LINT()

PY_SRCS(
    algorithms/__init__.py
    algorithms/base.py
    algorithms/realtime.py
    algorithms/simple.py
    algorithms/static_greedy.py
    algorithms/utils.py

    __init__.py
    common.py
    fields.py
    load.py
    models.py
    plot.py
)

END()

