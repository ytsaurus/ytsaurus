PY3TEST()

RESOURCE_FILES(
    "yt/yt/orm/codegen/mypy.ini"
)

PEERDIR(
    # MyPy testing harness.
    library/python/testing/types_test/py3

    # Targets to validate
    yt/yt/orm/codegen/bin
    yt/yt/orm/codegen/generator
    yt/yt/orm/codegen/model
)

TEST_SRCS(
    conftest.py
)

SIZE(MEDIUM)
TIMEOUT(600)

END()
