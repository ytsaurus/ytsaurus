GTEST(unittester-scheduler-simulator)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    operation_controller_ut.cpp
    control_thread_ut.cpp
)

ADDINCL(
    yt/yt/tools/scheduler_simulator
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tools/scheduler_simulator
    yt/yt/core/test_framework
)

END()
