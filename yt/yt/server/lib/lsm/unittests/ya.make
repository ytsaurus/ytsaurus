GTEST(unittester-library-lsm)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    upcoming_compaction_info_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/lsm
    yt/yt/server/lib/hydra
)

SIZE(SMALL)

END()
