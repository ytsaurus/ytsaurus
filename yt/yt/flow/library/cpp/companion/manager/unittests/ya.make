GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_entrypoint_ut.cpp
    config_ut.cpp
    java_companion_manager_ut.cpp
    java_process_manager_ut.cpp
    job_removal_ut.cpp
    jvm_options_ut.cpp
    process_manager_base_ut.cpp
    registry_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/companion/manager
    yt/yt/library/profiling/solomon
    yt/yt/library/query/engine
)

SIZE(SMALL)

END()
