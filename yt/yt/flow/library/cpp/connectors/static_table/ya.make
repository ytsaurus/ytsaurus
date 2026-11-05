LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    arrival_order_table_sink.cpp
    source.cpp
    source_spec.cpp
    spec.cpp
    GLOBAL register.cpp
)

PEERDIR(
    library/cpp/timezone_conversion
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/resources
    yt/yt/core
    yt/yt/client
    yt/yt/library/re2
)

END()

RECURSE_FOR_TESTS(unittests)
