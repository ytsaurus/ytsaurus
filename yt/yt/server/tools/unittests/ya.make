GTEST(unittester-tools)

ALLOCATOR(TCMALLOC)

SRCS(
    tools_ut.cpp
)

PEERDIR(
    yt/yt/server/tools
)

SIZE(MEDIUM)

END()
