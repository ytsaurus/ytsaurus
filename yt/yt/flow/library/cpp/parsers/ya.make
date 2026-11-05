LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    proto.cpp
    tskv.cpp
    json.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation

    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    unittests
)
