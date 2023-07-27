PY3_PROGRAM(yt-ipython)

OWNER(g:yt g:yt-python)

PEERDIR(
    yt/python/yt/wrapper

    yt/yt/python/yt_yson_bindings
    yt/yt/python/yt_driver_rpc_bindings

    contrib/python/ipython
)

PY_MAIN(IPython:start_ipython)

END()
