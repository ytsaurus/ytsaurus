LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    private.cpp
    shell.cpp
    shell_manager.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/library/containers
    yt/yt/library/process
    yt/yt/library/pipe_io
)

END()
