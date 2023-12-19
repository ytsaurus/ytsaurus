INCLUDE(../../pycxx.inc)

PY23_NATIVE_LIBRARY()

CXXFLAGS(
    ${PYCXX_FLAGS}
)

SRCS(
    descriptor.cpp
    driver.cpp
    error.cpp
    response.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/service_discovery/yp
    yt/yt/client/driver
    yt/yt/python/common
    yt/yt/python/yson
    contrib/libs/pycxx
)

ADDINCL(
    GLOBAL contrib/libs/pycxx
)

END()
