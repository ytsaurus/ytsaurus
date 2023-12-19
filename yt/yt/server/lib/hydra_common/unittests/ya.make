GTEST(unittester-hydra)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    checkpointable_stream_ut.cpp
    file_changelog_index_ut.cpp
    hydra_janitor_helpers_ut.cpp
    changelog_ut.cpp
    unbuffered_file_changelog_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/server/lib/hydra_common
)

SIZE(MEDIUM)

END()
