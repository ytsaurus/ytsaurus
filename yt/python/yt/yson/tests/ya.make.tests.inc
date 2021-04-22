
SET(YT_TEST_SRCDIR yt/python/yt/yson/tests)

SET(YT_TEST_SRCS
    test_common.py
    test_convert.py
    test_parser.py
    test_writer.py
    test_yson_types.py
)

PEERDIR(
    yt/python/yt/yson
    yt/python/yt/wrapper
    yt/yt/python/yt_yson_bindings
)
