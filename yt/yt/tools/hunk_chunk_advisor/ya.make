PY3_PROGRAM()

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
    yt/yt/python/yt_driver_bindings
    contrib/python/matplotlib
    contrib/libs/libc_compat/ubuntu_14
)

PY_SRCS(
    __main__.py
)

END()
