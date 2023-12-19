LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    program.cpp
    proc.cpp
    registry.cpp
    seccomp.cpp
    signaler.cpp
    tools.cpp
)

PEERDIR(
    yt/yt/library/process
    yt/yt/library/program

    yt/yt/core
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
)
