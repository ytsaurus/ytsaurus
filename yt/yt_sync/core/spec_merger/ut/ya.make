PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_preset_merger.py
    test_stages_merger.py
)

PEERDIR(
    yt/yt_sync/core/constants
    yt/yt_sync/core/spec_merger
)

END()
