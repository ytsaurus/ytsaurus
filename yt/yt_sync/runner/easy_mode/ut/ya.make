PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_make_description.py
)

PEERDIR(
    yt/yt_sync/runner/easy_mode

    library/python/resource

    contrib/python/pyaml
)

RESOURCE(
    yt/yt_sync/runner/easy_mode/ut/stages_example.yaml stages_example.yaml
)

END()
