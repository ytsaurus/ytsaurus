GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    registry_ut.cpp
    retrying_writer_ut.cpp
    spec_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/sorted_dynamic_table
)

SIZE(SMALL)

END()
