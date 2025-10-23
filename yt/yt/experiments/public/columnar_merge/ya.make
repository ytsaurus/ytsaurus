PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

ADDINCL(
    contrib/libs/sparsehash/src
    contrib/libs/re2
)

SRCS(
    main.cpp
    routines.cpp
)

PEERDIR(
    yt/yt/server/node
    yt/yt/server/lib/io

    yt/yt/ytlib

    yt/yt/library/query/row_comparer
    yt/yt/library/signals
    yt/yt/library/row_merger

    yt/yt/client

    yt/yt/core

    library/cpp/getopt
)

END()
