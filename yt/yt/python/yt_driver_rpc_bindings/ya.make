PY23_LIBRARY()

PEERDIR(yt/yt/python/driver/rpc)

PY_SRCS(
    NAMESPACE yt_driver_rpc_bindings

    __init__.py
    driver.py
)

END()
