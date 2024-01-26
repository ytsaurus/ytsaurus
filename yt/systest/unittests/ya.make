GTEST(unittester-yt-systest)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    stub_dataset.cpp

    sort_ut.cpp
    reduce_ut.cpp
    worker_set_ut.cpp

    util.cpp
)

PEERDIR(
    yt/systest
)

SIZE(SMALL)

END()
