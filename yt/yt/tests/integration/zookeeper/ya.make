PY3_LIBRARY()

TEST_SRCS(
    test_zookeeper.py
)

PEERDIR(
    contrib/python/kazoo
)

END()

RECURSE_FOR_TESTS(
    bin
)
