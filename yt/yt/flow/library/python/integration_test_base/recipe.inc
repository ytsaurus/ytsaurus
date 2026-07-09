# Everything an integration test that bootstraps pipelines via YtSync needs: the local YT
# recipe and the YtSync implementation — the real yt/yt_sync in Yandex-internal builds,
# the yt_sync_mini shim in opensource builds.

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

PEERDIR(
    yt/yt/flow/library/python/integration_test_base
)

IF (OPENSOURCE)
    PEERDIR(
        yt/yt/flow/library/python/yt_sync_mini
    )
ELSE()
    PEERDIR(
        yt/yt_sync/runner
    )
ENDIF()
