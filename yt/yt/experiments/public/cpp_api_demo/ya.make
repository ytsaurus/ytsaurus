PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    demo.cpp
)

PEERDIR(
    library/cpp/getopt
    yt/yt/core
    yt/yt/client
    yt/yt/ytlib
)

END()
