GTEST(unittester-distributed-chunk-session-server)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    distributed_chunk_session_sequencer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/server/lib/distributed_chunk_session_server
    yt/yt/ytlib
)

SIZE(SMALL)

END()
