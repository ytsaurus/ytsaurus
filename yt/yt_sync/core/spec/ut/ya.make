PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_consumer.py
    test_node.py
    test_pipeline.py
    test_producer.py
    test_schema.py
    test_table.py
)

PEERDIR(
    yt/yt_sync/core/constants
    yt/yt_sync/core/spec
    yt/yt_sync/core/fixtures
)

END()
