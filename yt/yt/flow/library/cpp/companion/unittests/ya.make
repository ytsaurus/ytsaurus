GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_client_ut.cpp
    companion_computation_base_ut.cpp
    companion_entrypoint_ut.cpp
    companion_model_ut.cpp
    companion_proxy_ut.cpp
    companion_resource_ut.cpp
    config_ut.cpp
    java_companion_manager_ut.cpp
    java_process_manager_ut.cpp
    job_removal_ut.cpp
    jvm_options_ut.cpp
    process_manager_base_ut.cpp
    registry_ut.cpp
    transform_ordered_source_companion_computation_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/companion
    yt/yt/library/query/engine
)

SIZE(SMALL)

END()
