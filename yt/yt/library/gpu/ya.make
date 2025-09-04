LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    gpu_info_provider.cpp
    gpu_info_provider_detail.cpp
    helpers.cpp
    gpu_agent_gpu_info_provider.cpp
    nv_manager_gpu_info_provider.cpp
    nvidia_smi_gpu_info_provider.cpp
)

PEERDIR(
    yt/yt/library/process

    yt/yt/core
    yt/yt/core/rpc/grpc

    yt/yt/gpuagent/api

    library/cpp/protobuf/interop
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)
