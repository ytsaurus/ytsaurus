GTEST(library-gpu-unittest)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

SRC(
    gpu_agent_gpu_info_provider_ut.cpp
)

PEERDIR(
    yt/yt/library/gpu

    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
