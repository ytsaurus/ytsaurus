LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    public.cpp
    config.cpp
    helpers.cpp
    job_report.cpp

    proto/supervisor_service.proto
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/server/lib/job_agent
    yt/yt/server/lib/job_proxy
    yt/yt/library/containers
    yt/yt/server/lib/misc
)

IF (NOT OPENSOURCE)
    BUNDLE(
        yt/go/tar2squash
    )
    RESOURCE(
        tar2squash /tar2squash
    )
ENDIF()

END()
