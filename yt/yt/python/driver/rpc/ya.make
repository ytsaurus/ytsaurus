INCLUDE(../../pycxx.inc)

PY23_NATIVE_LIBRARY()

CXXFLAGS(
    ${PYCXX_FLAGS}
)

SRCS(
    GLOBAL driver_rpc.cpp
)

PEERDIR(
    yt/yt/python/driver/lib
)

IF(USE_ARCADIA_PYTHON)
    PY_REGISTER(driver_rpc_lib)
ENDIF()

END()
