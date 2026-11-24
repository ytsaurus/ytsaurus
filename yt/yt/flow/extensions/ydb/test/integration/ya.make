PY3TEST()

PY_SRCS(
    ../acquire_server.py=public_fixture.acquire_server
    ../federation_discovery_mock.py=public_fixture.federation_discovery_mock
    ../public_ydb.py=public_fixture.public_ydb
    ../ydb_outage_proxy.py=public_fixture.ydb_outage_proxy
    lenval.py=public_fixture.lenval
)

TEST_SRCS(
    test_ydb.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

PEERDIR(
    contrib/python/grpcio
    contrib/python/requests
    contrib/python/ydb
    library/python/port_manager
    yt/yt/flow/extensions/ydb/python
    yt/yt/flow/extensions/ydb/test/proto
    yt/yt/flow/library/python/bullied_process
    yt/yt/flow/library/python/integration_test_base
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
    yt/yt/flow/library/python/queue
)

DEPENDS(
    yt/yt/flow/bin/flow_server
    yt/yt/flow/extensions/ydb/cpp/unittests
    yt/yt/flow/extensions/ydb/test/pipeline
    yt/yt/flow/extensions/ydb/test/probe
    yt/yt/flow/extensions/ydb/test/ydbd
)

DATA(
    arcadia/yt/yt/flow/extensions/ydb/test/pipeline/delivery_pipeline.yson
    arcadia/yt/yt/flow/extensions/ydb/test/pipeline/pipeline.yson
    arcadia/yt/yt/flow/extensions/ydb/test/server.lock.json
    arcadia/yt/yt/flow/extensions/ydb/test/server_config.yaml
)

IF (OPENSOURCE)
    REQUIREMENTS(network:full)
ENDIF()

REQUIREMENTS(
    cpu:4
    ram:32
)

FORK_SUBTESTS()

SPLIT_FACTOR(48)

SIZE(MEDIUM)

END()
