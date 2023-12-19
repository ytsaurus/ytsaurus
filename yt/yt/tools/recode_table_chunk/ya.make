PROGRAM(recode-table-chunk)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
    yt/yt/server/lib/io
    yt/yt/library/program
    library/cpp/getopt/small
)

END()
