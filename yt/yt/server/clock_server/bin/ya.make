PROGRAM(ytserver-clock)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(g:yt)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache
    yt/yt/server/clock_server
    library/cpp/getopt
)

END()
