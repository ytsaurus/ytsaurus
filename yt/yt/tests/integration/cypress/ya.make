PY3_LIBRARY()

TEST_SRCS(
    test_cypress_acls.py
    test_cypress_annotations.py
    test_cypress_locks.py
    test_cypress_request_limits.py
    test_cypress.py
    test_portals.py
)

END()

RECURSE_FOR_TESTS(
    bin
)
