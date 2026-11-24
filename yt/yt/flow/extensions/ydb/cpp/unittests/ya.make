GTEST(unittester-flow-ydb)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    credentials_ut.cpp
    deferred_publication_sink_ut.cpp
    deferred_publication_ut.cpp
    ordinary_sink_contract_ut.cpp
    registry_ut.cpp
    sink_ut.cpp
    source_ut.cpp
    spec_ut.cpp
    topic_info_ut.cpp
)

PEERDIR(
    yt/yt/flow/extensions/ydb/cpp
    yt/yt/flow/extensions/ydb/cpp/unittests/lib
    yt/yt/flow/extensions/ydb/sdk
    yt/yt/core/test_framework
)

DATA(
    arcadia/yt/yt/flow/extensions/ydb/sdk/epilogue.cmake
    arcadia/yt/yt/flow/extensions/ydb/sdk/vanilla_protobuf_out.cpp
)

END()

RECURSE(
    lib
)
