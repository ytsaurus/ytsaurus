LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    serializer.cpp
    state.cpp
    validate.cpp
)

PEERDIR(
    library/cpp/yt/yson
    yt/yt/client
    yt/yt/flow/library/cpp/delta_codecs
)

END()

RECURSE_FOR_TESTS(unittests)
