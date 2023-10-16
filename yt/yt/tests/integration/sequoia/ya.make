PY3_LIBRARY()

TEST_SRCS(
    test_grafting.py
    test_sequoia.py
    test_sequoia_objects.py
)

END()

RECURSE_FOR_TESTS(
    bin
)
