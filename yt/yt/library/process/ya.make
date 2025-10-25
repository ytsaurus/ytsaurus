LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    process.cpp
    subprocess.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/pipe_io
)

END()

RECURSE_FOR_TESTS(
    unittests
)
