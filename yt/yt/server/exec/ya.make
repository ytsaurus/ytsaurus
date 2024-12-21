LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/server/lib/user_job

    yt/yt/library/program
    yt/yt/library/process

    yt/yt/core
)

SRCS(
    program.cpp
    user_job_synchronizer.cpp
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()

