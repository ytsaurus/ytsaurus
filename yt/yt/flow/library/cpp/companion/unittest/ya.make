GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_client_ut.cpp
    companion_entrypoint_ut.cpp
    companion_proxy_ut.cpp
    config_ut.cpp
    java_companion_manager_ut.cpp
    java_process_manager_ut.cpp
    jvm_options_ut.cpp
    process_manager_base_ut.cpp
    swift_ordered_source_companion_computation_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/companion
)

SIZE(SMALL)

END()
