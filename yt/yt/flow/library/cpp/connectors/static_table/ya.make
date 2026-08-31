LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    arrival_order_table_sink.cpp
    spec.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/connectors/static_table_v2
    yt/yt/flow/library/cpp/resources
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(unittests)
