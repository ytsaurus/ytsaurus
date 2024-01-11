PROGRAM(lsm-sim)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/experiments/public/lsm_simulator/lib
)

END()
