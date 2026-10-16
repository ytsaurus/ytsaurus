FUZZ()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

TAG(
    ya:fat
)

SRCS(
    cow_tree_fuzz.cpp
)

PEERDIR(
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/misc
)

SIZE(LARGE)

END()
