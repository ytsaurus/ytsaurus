PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

TEST_SRCS(
    test_constants_drift.py
    test_easy_mode.py
    test_preset_merger.py
    test_yt_sync_mini.py
)

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/flow/library/python/yt_sync_mini
    yt/yt/flow/library/python/pipeline_tables
)

# For the constants drift test only; skipped when yt_sync is absent.
IF (NOT OPENSOURCE)
    PEERDIR(
        yt/yt_sync/core/constants
    )
ENDIF()

SIZE(MEDIUM)

END()
