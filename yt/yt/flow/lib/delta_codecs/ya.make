LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    codec.cpp
    none.cpp
    state.cpp
    xdelta.cpp
)

PEERDIR(
    contrib/libs/xdelta3
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/error
)

END()

RECURSE_FOR_TESTS(
    unittests
)
