LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/cpp/mapreduce/interface
    library/cpp/testing/gtest_extensions
)

SRCS(
    yt_mock.cpp
)

END()
