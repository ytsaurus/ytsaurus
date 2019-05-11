INCLUDE(../../YaMakeBoilerplate.txt)
INCLUDE(../pycxx.inc)

PY_ANY_MODULE(yson_lib PREFIX "")

CXXFLAGS(
    ${YT_ARCADIA_CXXFLAGS}
    ${PYCXX_FLAGS}
)
CFLAGS(${YT_ARCADIA_CFLAGS})

PEERDIR(
    ${YT_ROOT}/yt/core
    ${YT_ROOT}/yt/python/common
    ${YT_ROOT}/yt/python/yson
)

