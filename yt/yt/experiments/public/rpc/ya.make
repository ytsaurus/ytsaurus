PROGRAM(rpc)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

PROTO_NAMESPACE(yt)

SRCS(
    main.cpp
    main.proto
)

PEERDIR(
    yt/yt/core
    library/cpp/yt/phdr_cache
    yt/yt/ytlib
    yt/yt/server/lib
    library/cpp/getopt/small
    library/cpp/deprecated/atomic
)

END()
