PY23_TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

TEST_SRCS(
    __init__.py
    conftest.py
    test_clickhouse.py
    test_controller.py
)

PEERDIR(
    yt/python/yt/clickhouse
    yt/python/yt/testlib
)

DEPENDS(
    yt/yt/packages/tests_package
    yt/yt/experiments/public/ytserver_dummy

    yt/chyt/controller/cmd/chyt-controller
    yt/chyt/server/bin
    yt/chyt/trampoline
    yt/yt/server/log_tailer/bin

    yt/python/yt/wrapper/bin/yt_make
)

DATA(
    arcadia/yt/python/yt/clickhouse/tests/test_cli.sh
)

REQUIREMENTS(
    ram_disk:4
    cpu:4
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:20)
ENDIF()

TAG(ya:yt ya:fat)
YT_SPEC(yt/yt/tests/integration/spec.yson)
SIZE(LARGE)

TAG(
    ya:huge_logs
    ya:full_logs
    ya:sys_info
)

END()
