LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    # GLOBAL prevents linker from dropping strong GetErrorSkeleton symbol.
    # Don't ask me how this helps :(
    GLOBAL skeleton.cpp
)

PEERDIR(
    yt/yt/core
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    unittests
)
