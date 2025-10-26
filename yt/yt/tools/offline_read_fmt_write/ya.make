PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/yt/phdr_cache
    yt/yt/library/pipe_io
    yt/yt/client
    yt/yt/server/lib/io
    yt/yt/ytlib
    library/cpp/getopt/small
)

ADDINCL(
    contrib/libs/sparsehash/src
)

END()
