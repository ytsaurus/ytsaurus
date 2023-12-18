PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/yt/phdr_cache
    yt/yt/client
    yt/yt/server/lib/io
    yt/yt/ytlib
    yt/yt/library/query/engine
    library/cpp/getopt/small
)

ADDINCL(
    contrib/libs/sparsehash/src
)

END()
