GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    block_builder_ut.cpp
    classification_ut.cpp
    host_form_ut.cpp
    init_ut.cpp
    recovery_ut.cpp
    registry_ut.cpp
    rollback_compatibility_ut.cpp
    shard_ut.cpp
    tls_ut.cpp
    topology_guard_ut.cpp
    writer_ut.cpp
)

PEERDIR(
    yt/yt/flow/extensions/clickhouse/cpp
    yt/yt/flow/library/cpp/common/unittests/mock
    contrib/libs/openssl
    library/cpp/testing/common
)

DATA(
    arcadia/yt/yt/flow/extensions/clickhouse/cpp/unittests/testdata/server.crt
    arcadia/yt/yt/flow/extensions/clickhouse/cpp/unittests/testdata/server.key
)

SIZE(SMALL)

END()
