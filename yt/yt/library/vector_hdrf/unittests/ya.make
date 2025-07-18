GTEST(unittester-fair-share-update)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    fair_share_update_ut.cpp
)

SET(TEST_DATA_DIR ${ARCADIA_ROOT}/yt/yt/library/vector_hdrf/unittests/data)

RESOURCE(
    # Build by fetch_scheduling_data script with production GPU tree.
    ${TEST_DATA_DIR}/gpu_tree_elements.yson /data/gpu_tree_elements.yson
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/vector_hdrf
    yt/yt/core/test_framework
)

END()

RECURSE(
    fetch_scheduling_data
)
