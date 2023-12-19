GTEST(unittester-python)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    dynamic_ring_buffer_ut.cpp
    stream_ut.cpp
    tee_input_stream_ut.cpp
)

USE_PYTHON3()

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/core
    yt/yt/python/common
)

END()
