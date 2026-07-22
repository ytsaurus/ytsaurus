PY3_LIBRARY()

TEST_SRCS(
    test_environment.py
    test_mock.py
    test_ql.py
)

PEERDIR(
    yt/yt/tests/conftest_lib
)

END()

RECURSE_FOR_TESTS(
    bin
)
