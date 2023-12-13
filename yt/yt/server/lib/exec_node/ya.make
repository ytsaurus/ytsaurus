LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    public.cpp
    config.cpp
    job_report.cpp
    gpu_helpers.cpp
    helpers.cpp

    proto/supervisor_service.proto
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/server/lib/job_agent
    yt/yt/server/lib/job_proxy
    yt/yt/server/lib/nbd
    yt/yt/library/containers
    yt/yt/library/containers/cri
    yt/yt/library/gpu
    yt/yt/server/lib/misc
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
