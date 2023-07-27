OWNER(nslus)

PY23_LIBRARY()

TEST_SRCS(test_retry.py)

PEERDIR(
    library/python/retry
)

END()

RECURSE_FOR_TESTS(
    py2
    py3
)
