GTEST(unittester-flow-state-unpack)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(state_unpack_ut.cpp)

RESOURCE(
    ${ARCADIA_ROOT}/yt/yt/flow/tests/data/internal_state_rows.yson flow_internal_state_rows
)

PEERDIR(
    library/cpp/resource
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/tables
)

END()
