LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    GLOBAL configure_pipe_io_dispatcher.cpp
    pipe_io_dispatcher.cpp
    pipe.cpp
    pty.cpp
)

PEERDIR(
    yt/yt/core
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    unittests
)
