PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    yt/yt/core
    yt/yt/server/lib/io
)

END()
