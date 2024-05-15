PROGRAM(parse-rpc-header)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt/small
    library/cpp/yt/memory
    library/cpp/yt/string
    yt/yt/core
    yt/yt/library/program
    yt/yt_proto/yt/core
)

END()
