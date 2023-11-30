GTEST(unittester-yql-agent)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    yql_agent_ut.cpp
)

PEERDIR(
    yt/yql/agent

    yt/yt/library/named_value
)

SIZE(MEDIUM)

END()
