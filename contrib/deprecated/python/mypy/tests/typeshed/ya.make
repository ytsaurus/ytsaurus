PY3TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/deprecated/python/mypy
    library/python/testing/yatest_common
)

NO_LINT()

SRCDIR(contrib/deprecated/python/mypy)

PY_SRCS(
    TOP_LEVEL
    # mypy/typeshed/scripts/migrate_script.py
    # mypy/typeshed/scripts/update-stubtest-whitelist.py
    mypy/typeshed/tests/check_consistent.py
    # mypy/typeshed/tests/mypy_self_check.py
    mypy/typeshed/tests/mypy_test.py
    # mypy/typeshed/tests/mypy_test_suite.py
    # mypy/typeshed/tests/pytype_test.py
    # mypy/typeshed/tests/stubtest_test.py
    # mypy/typeshed/tests/stubtest_unused.py
)

TEST_SRCS(
    test_typeshed.py
)

DATA(
    arcadia/contrib/deprecated/python/mypy/mypy/typeshed/stdlib
    arcadia/contrib/deprecated/python/mypy/mypy/typeshed/third_party
)

END()
