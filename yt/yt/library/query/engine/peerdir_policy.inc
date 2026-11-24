# ytlib normally uses engine_api; these consumers also need the engine.
PEERDIR_FROM_POLICY(
    ALLOW
    yt/yt/ytlib/fuzz/arrow
    yt/yt/ytlib/table_client/unittests
    yt/yt/ytlib/test_framework
    yt/yt/ytlib/unittests
)

PEERDIR_FROM_POLICY(
    DENY
    yt/yt/ytlib
)
