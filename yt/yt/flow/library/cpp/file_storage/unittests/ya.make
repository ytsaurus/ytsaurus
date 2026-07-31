GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    file_storage_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/file_storage
)

END()
