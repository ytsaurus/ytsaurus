INCLUDE(../../pycxx.inc)

PY_ANY_MODULE(driver_lib PREFIX "")

CXXFLAGS(
    ${PYCXX_FLAGS}
)

PEERDIR(
    yt/yt/python/driver/native
)
