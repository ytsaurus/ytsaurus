PROGRAM(rpc)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

PROTO_NAMESPACE(yt)

SRCS(
    main.cpp
    main.proto
)

PEERDIR(
    yt/yt/core
    library/cpp/getopt/small
)

END()
