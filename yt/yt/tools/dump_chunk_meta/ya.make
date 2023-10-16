PROGRAM(dump-chunk-meta)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/server/lib/io
    yt/yt/ytlib
    yt/yt/core
    yt/yt/library/program
    library/cpp/getopt/small
)

END()
