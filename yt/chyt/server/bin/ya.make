PROGRAM(ytserver-clickhouse)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

ALLOCATOR(TCMALLOC_256K)

CXXFLAGS(-fno-inline)

SRCS(
    main.cpp
)

PEERDIR(
    # Clickhouse already has its own implementation of phdr_cache (which is kinda inspired by ours :)
    # yt/yt/library/phdr_cache
    yt/yt/library/re2
    yt/chyt/server
    library/cpp/getopt
)

NO_EXPORT_DYNAMIC_SYMBOLS()

END()
