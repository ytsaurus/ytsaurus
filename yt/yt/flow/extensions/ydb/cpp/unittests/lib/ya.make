LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    deferred_publication_contract.cpp
    fakes.cpp
    ordinary_sink_contract_helpers.cpp
    sink_test_environment.cpp
    source_contract.cpp
)

PEERDIR(
    library/cpp/framing
    yt/yt/flow/extensions/ydb/cpp
    yt/yt/flow/extensions/ydb/sdk
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/core/test_framework
)

END()
