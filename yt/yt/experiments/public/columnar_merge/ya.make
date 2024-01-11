PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

ADDINCL(
    contrib/libs/sparsehash/src
    contrib/libs/re2
)

SRCS(
    main.cpp
    routines.cpp
)

PEERDIR(
    yt/yt/library/query/row_comparer
    yt/yt/core
    yt/yt/client
    yt/yt/ytlib
    yt/yt/server/node
    yt/yt/server/lib/io
    library/cpp/getopt
)

END()
