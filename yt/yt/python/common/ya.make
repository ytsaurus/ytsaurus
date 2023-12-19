INCLUDE(../pycxx.inc)

PY23_NATIVE_LIBRARY()

CXXFLAGS(
    ${PYCXX_FLAGS}
)

SRCS(
    buffered_stream.cpp
    cache.cpp
    dynamic_ring_buffer.cpp
    error.cpp
    helpers.cpp
    shutdown.cpp
    stream.cpp
    tee_input_stream.cpp
)

PEERDIR(
    yt/yt/core
)

PEERDIR(
    contrib/libs/pycxx
)
ADDINCL(
    GLOBAL contrib/libs/pycxx
)

END()
