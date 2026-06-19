PROGRAM(logslice)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

STRIP()

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/tools/logslice
    library/cpp/getopt/small
)

END()
