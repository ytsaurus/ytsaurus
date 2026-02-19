GTEST(unittester-library-lsm)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    row_digest_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/lsm
    yt/yt/server/lib/hydra
)

SIZE(SMALL)

END()
