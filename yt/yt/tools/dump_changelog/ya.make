PROGRAM(dump-changelog)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/program
    yt/yt/server/lib
    yt/yt/server/lib/hydra
    yt/yt/ytlib
    library/cpp/getopt/small
)

END()
