INCLUDE(../../../YaMakeBoilerplate.txt)
INCLUDE(../../pycxx.inc)

PY_ANY_MODULE(driver_rpc_lib PREFIX "")

CXXFLAGS(
    ${YT_ARCADIA_CXXFLAGS}
    ${PYCXX_FLAGS}
)

PEERDIR(
    ${YT_ROOT}/yt/python/driver/rpc
)
