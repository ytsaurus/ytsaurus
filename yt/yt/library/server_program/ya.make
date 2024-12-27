LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache
    library/cpp/yt/mlock

    yt/yt/core/service_discovery/yp # for YPSD singleton

    yt/yt/library/program
    yt/yt/library/containers
    yt/yt/library/disk_manager
    yt/yt/library/stockpile # for stockpile singleton
)

END()
