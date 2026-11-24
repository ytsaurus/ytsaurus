PY3TEST()

TEST_SRCS(
    acquire_server.py
    public_ydb.py
    test_fixture.py
)

PEERDIR(
    contrib/python/ydb
    library/python/port_manager
)

DEPENDS(
    yt/yt/flow/extensions/ydb/test/auth_probe
    yt/yt/flow/extensions/ydb/test/probe
    yt/yt/flow/extensions/ydb/test/ydbd
)

DATA(
    arcadia/yt/yt/flow/extensions/ydb/test/server.lock.json
    arcadia/yt/yt/flow/extensions/ydb/test/server_config.yaml
)

IF (OPENSOURCE)
    REQUIREMENTS(network:full)
ENDIF()

REQUIREMENTS(
    cpu:4
    ram:16
)

SIZE(MEDIUM)

END()

RECURSE(
    auth_probe
    integration
    pipeline
    probe
    proto
    ydbd
)
