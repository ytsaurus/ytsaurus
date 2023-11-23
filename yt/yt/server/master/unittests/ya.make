GTEST(unittester-master)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    chunk_helpers.cpp
    chunk_list_statistics_ut.cpp
    chunk_replacer_ut.cpp
    chunk_requisition_ut.cpp
    chunk_tree_balancer_ut.cpp
    chunk_tree_traversing_ut.cpp
    chunk_view_ut.cpp
    cumulative_statistics_ut.cpp
    helpers.cpp
    incumbent_scheduler_ut.cpp
    interned_pool_attributes_ut.cpp
    tablet_cell_balancer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/master
)

SIZE(MEDIUM)

END()
