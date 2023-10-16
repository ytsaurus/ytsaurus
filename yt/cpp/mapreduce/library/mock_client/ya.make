LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/cpp/mapreduce/interface
    library/cpp/testing/gmock_in_unittest
)

SRCS(
    yt_mock.cpp
)

END()
