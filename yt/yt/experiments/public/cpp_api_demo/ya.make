PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

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
