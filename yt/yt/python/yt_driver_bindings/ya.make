PY23_LIBRARY()

PEERDIR(yt/yt/python/driver/native)

PY_SRCS(
    NAMESPACE yt_driver_bindings

    __init__.py
    driver.py
)

END()
