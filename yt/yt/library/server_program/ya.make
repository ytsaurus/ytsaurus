LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    server_program.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache
    library/cpp/yt/mlock
    yt/yt/library/program
)

END()
