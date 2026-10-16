PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

TEST_SRCS(
    base.py
    test_environment.py
    test_mock.py
    test_ql.py
)

PEERDIR(
    yt/yt/tests/conftest_lib
    contrib/python/pyarrow
)

END()

RECURSE_FOR_TESTS(
    bin
)
