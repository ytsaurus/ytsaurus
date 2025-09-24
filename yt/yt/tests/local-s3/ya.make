PY3_PROGRAM(local_s3_recipe)

SRCDIR(
    yt/yt/tests/local-s3
)

PY_SRCS(__main__.py)

PEERDIR(
    yt/python/yt/test_helpers
    library/python/testing/recipe
    library/python/resource

    contrib/python/boto3
)

END()
