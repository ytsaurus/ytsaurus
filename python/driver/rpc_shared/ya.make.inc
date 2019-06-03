INCLUDE(../../../YaMakeBoilerplate.txt)
INCLUDE(../../pycxx.inc)

PY_ANY_MODULE(driver_rpc_lib PREFIX "")

CXXFLAGS(
    ${YT_ARCADIA_CXXFLAGS}
    ${PYCXX_FLAGS}
)
CFLAGS(${YT_ARCADIA_CFLAGS})

PEERDIR(
    ${YT_ROOT}/yt/python/driver/rpc
)
