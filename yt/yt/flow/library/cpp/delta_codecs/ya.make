LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    codec.cpp
    none.cpp
    state.cpp
    vcdiff.cpp
    xdelta.cpp
)

PEERDIR(
    contrib/libs/xdelta3
    contrib/tools/open-vcdiff
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/error
)

END()

RECURSE_FOR_TESTS(
    unittests
)
