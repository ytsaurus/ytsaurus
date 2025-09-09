LIBRARY()

SRCS(
    serializer.cpp
    state.cpp
)

PEERDIR(
    library/cpp/yt/yson
    yt/yt/client
    yt/yt/flow/lib/delta_codecs
)

END()

RECURSE_FOR_TESTS(unittests)
