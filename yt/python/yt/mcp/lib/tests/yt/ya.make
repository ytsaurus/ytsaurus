PY3TEST()

PEERDIR(
    yt/python/yt/mcp/lib
    yt/python/yt/wrapper/testlib

    contrib/python/pytest
)

TEST_SRCS(
    conftest.py
    test_tools.py
)

DEPENDS(
    yt/yt/packages/tests_package
)

END()
