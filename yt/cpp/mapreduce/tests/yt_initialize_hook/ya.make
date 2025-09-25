LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/testing/hook
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/client
)

SRCS(
    GLOBAL yt_hook.cpp
)

END()
