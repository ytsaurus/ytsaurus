PY3_LIBRARY()

TEST_SRCS(
    test_s3_medium.py
)

PEERDIR(
    contrib/python/boto3
    contrib/python/pandas
    contrib/python/pyarrow
)

END()

RECURSE_FOR_TESTS(
    bin
)
