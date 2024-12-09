PY3_PROGRAM(yt-python3)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PY_MAIN(:main)

PEERDIR(
    contrib/python/pytest
    contrib/python/pytest-mock
    contrib/python/execnet
    contrib/python/flaky
    contrib/python/ipython

    yt/python/yt
    yt/python/yt/cli
    yt/python/yt/yson
    yt/python/yt/clickhouse
    yt/python/yt/wrapper
    yt/python/yt/wrapper/testlib
    yt/python/yt/environment
    yt/python/yt/testlib

    yt/yt/python/yt_yson_bindings
    yt/yt/python/yt_driver_bindings
)

END()
