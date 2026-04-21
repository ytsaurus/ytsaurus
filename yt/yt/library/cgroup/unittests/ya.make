GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    process_ut.cpp
    statistics_ut.cpp
)

PEERDIR(
    yt/yt/library/cgroup
)

END()
