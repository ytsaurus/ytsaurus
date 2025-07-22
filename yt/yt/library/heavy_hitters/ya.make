LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    misra_gries.cpp
)

PEERDIR(
    library/cpp/yt/memory
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
