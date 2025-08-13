PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/server/controller_agent
    yt/yt/ytlib
    library/cpp/getopt/small
    yt/yt/library/coredumper
    yt/yt/library/signals
)

END()
