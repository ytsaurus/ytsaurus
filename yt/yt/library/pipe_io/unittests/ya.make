GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    pipes_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/backtrace_introspector
    yt/yt/library/pipe_io
    yt/yt/library/signals
)

SIZE(MEDIUM)

END()
