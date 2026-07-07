LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/client/formats
    yt/yt/library/formats
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
