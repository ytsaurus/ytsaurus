LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    config.cpp
    estimate_size_helpers.cpp
    gpu_helpers.cpp
    gpu_info_provider.cpp
    structs.cpp
)

PEERDIR(
    library/cpp/protobuf/interop

    yt/yt/ytlib
    yt/yt/server/lib/job_proxy
    yt/yt/server/lib/misc
)

IF (NOT OPENSOURCE)
    SRCS(
        GLOBAL gpu_info_provider_impl.cpp
    )
    PEERDIR(
        infra/rsm/nvgpumanager/api
    )
ENDIF()

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        unittests
    )
ENDIF()
