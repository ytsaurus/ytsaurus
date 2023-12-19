PROGRAM(tablet-balancer-dry-mode)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/server/lib/tablet_balancer/dry_run/lib
    library/cpp/getopt/small
    library/cpp/yt/logging
)

END()
