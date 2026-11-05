GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_computation_base_ut.cpp
    companion_spec_validation_ut.cpp
    registry_ut.cpp
    transform_ordered_source_companion_computation_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/companion
    yt/yt/library/profiling/solomon
    yt/yt/library/query/engine
)

SIZE(SMALL)

END()
