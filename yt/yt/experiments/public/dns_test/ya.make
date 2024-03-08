PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
    library/cpp/getopt
    library/cpp/yt/misc
)

END()
