PY3_PROGRAM(yt-cli-all)

PEERDIR(
    library/python/resource
    yt/python/yt/cli
    yt/python/yt/wrapper
    yt/yt/python/yt_driver_bindings
    yt/yt/python/yt_driver_rpc_bindings
)

COPY_FILE(yt/python/yt/wrapper/bin/yt-admin yt_admin.py)

RESOURCE(
    fish_hunts_crickets.py /fish_hunts_crickets.py
)

PY_SRCS(
    __main__.py
    yt_admin.py
)

END()
