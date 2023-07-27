PEERDIR(
    library/python/cyson
)

IF(NOT PYTHON3 OR NOT OS_WINDOWS)
    PEERDIR(
        contrib/python/numpy
    )
ENDIF()

TEST_SRCS(
    library/python/cyson/ut/common/test_control_attributes.py
    library/python/cyson/ut/common/test_input_stream.py
    library/python/cyson/ut/common/test_py_reader_writer.py
    library/python/cyson/ut/common/test_reader_writer.py
    library/python/cyson/ut/common/test_unsigned_long.py
)
