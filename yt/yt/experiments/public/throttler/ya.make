PROGRAM(throttler)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core

    library/cpp/getopt/small
)

END()
