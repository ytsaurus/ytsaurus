INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

SET(YT_TEST_SRCDIR yt/python/yt/yson/tests)

SET(YT_TEST_SRCS
    test_common.py
    test_convert.py
    test_parser.py
    test_writer.py
    test_yson_types.py
)

PEERDIR(
    ${YT_PYTHON_ROOT}/python/yt/yson
    ${YT_PYTHON_ROOT}/python/yt/wrapper
)

NO_LINT()
