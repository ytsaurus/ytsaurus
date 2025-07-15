PROGRAM(zstdtail)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/program
    yt/yt/tools/zstdtail/lib
    library/cpp/getopt
)

END()
