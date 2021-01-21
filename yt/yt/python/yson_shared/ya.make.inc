INCLUDE(../../YaMakeBoilerplate.txt)
INCLUDE(../pycxx.inc)

PY_ANY_MODULE(yson_lib PREFIX "")

CXXFLAGS(
    ${YT_ARCADIA_CXXFLAGS}
    ${PYCXX_FLAGS}
)

PEERDIR(
    yt/yt/core
    yt/yt/python/common
    yt/yt/python/yson
)

