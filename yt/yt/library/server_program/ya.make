LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache
    library/cpp/yt/mlock
    yt/yt/library/program
    yt/yt/library/containers
    yt/yt/library/disk_manager
)

END()
