PY3TEST()

SIZE(LARGE)

IF (AUTOCHECK OR YT_TEAMCITY)
    FORK_SUBTESTS()

    SPLIT_FACTOR(30)

    TIMEOUT(1800)
ENDIF()

YT_SPEC(yt/yt/tests/integration/spec.yson)

TAG(
    ya:fat
    ya:full_logs
    ya:noretries
    ya:yt
    ya:ytexec
    ya:force_sandbox
)

REQUIREMENTS(
    sb_vault:YT_TOKEN=value:ignat:robot-yt-test-token
    cpu:10
    ram:32
    ram_disk:4
)

DEPENDS(
    yt/yt/packages/tests_package
)

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/wrapper/testlib
    yt/python/yt/testlib
    yt/python/yt/yson
    yt/yt/python/yson/arrow
    yt/yt/python/yt_yson_bindings

    contrib/python/flaky
    contrib/python/pyarrow
    contrib/python/pandas
)

COPY_FILE(../conftest.py conftest.py)

TEST_SRCS(
    conftest.py
    test_arrow.py
    test_typed_api.py
    test_structured_skiff.py
    test_validation_schema.py
    test_parquet.py
)

END()
