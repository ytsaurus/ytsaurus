INCLUDE(../../pycxx.inc)

PY23_NATIVE_LIBRARY()

CXXFLAGS(
    ${PYCXX_FLAGS}
)

SRCS(
    GLOBAL driver.cpp
)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/python/driver/lib
    yt/yt/ytlib
    yt/yt/server/lib/chunk_pools
)

IF(USE_ARCADIA_PYTHON)
    PY_REGISTER(driver_lib)
ENDIF()

END()
