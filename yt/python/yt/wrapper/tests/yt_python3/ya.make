PROGRAM(yt-python3)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

USE_PYTHON3()

OWNER(g:contrib orivej)

SRCDIR(contrib/tools/python3)

SRCS(
    src/Programs/python.c
)

PEERDIR(
    contrib/tools/python3/lib/py
    contrib/tools/python3/src/Lib
    contrib/python/pytest
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
    yt/yt/python/yt_driver_rpc_bindings
)

CFLAGS(
    -DPy_BUILD_CORE
)

END()
