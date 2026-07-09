LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_client_detail.cpp
    companion_entrypoint.cpp
    companion_manager.cpp
    companion_model.cpp
    java_companion_manager.cpp
    java_process_manager.cpp
    companion_process_manager.cpp
    companion_proxy.cpp
    companion_singleton_state.cpp
    config.cpp
    jvm_options.cpp
    process_manager_base.cpp
    GLOBAL register.cpp
    swift_map_companion_computation.cpp
    swift_ordered_source_companion_computation.cpp
    transform_companion_computation.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/proto

    yt/yt/library/process

    yt/yt/core/rpc/grpc

    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/misc
)

END()

RECURSE_FOR_TESTS(
    unittest
)
