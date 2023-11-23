GTEST(unittester-controller-agent)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    auto_merge_ut.cpp
    job_monitoring_index_manager_ut.cpp
    job_splitter_ut.cpp
    partitions_ut.cpp
    docker_image_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/controller_agent
    yt/yt/server/lib/chunk_pools/mock
)

SIZE(MEDIUM)

END()
