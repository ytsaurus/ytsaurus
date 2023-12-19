PROGRAM(gdbtest)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

CFLAGS(
    -g
)

SRCS(
    main.cpp
    foo.cpp
    bar.cpp
)

PEERDIR(
    yt/yt/core
)

END()
