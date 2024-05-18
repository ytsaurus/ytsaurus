PY3_LIBRARY()

TEST_SRCS(
    test_kafka.py
)

PEERDIR(
    contrib/python/confluent-kafka
)

END()

RECURSE_FOR_TESTS(
    bin
)
