PY3_LIBRARY()

# For opensource.
PY_SRCS(
    __init__.py
)

TEST_SRCS(
    protobuf_format.py
    test_arrow_format.py
    test_dsv_format.py
    test_protobuf_format.py
    test_skiff_format.py
    test_json_format.py
    test_web_json_format.py
    test_yson_format.py
)

PEERDIR(
    contrib/python/pyarrow
)

END()

RECURSE_FOR_TESTS(
    bin
)
