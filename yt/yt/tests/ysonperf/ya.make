PY3TEST()

SIZE(LARGE)

TAG(
    ya:fat
    ya:full_logs
    ya:sys_info
    ya:force_sandbox
    ya:sandbox_coverage
    ya:noretries
    ya:yt
    ya:relwithdebinfo
    ya:dump_test_env
    ya:large_tests_on_single_slots
)
    
YT_SPEC(yt/yt/tests/integration/spec.yson)

REQUIREMENTS(
    yav:YT_TOKEN=value:sec-01gg4hcd881t1nncr04nmbeg04:yt_token
    yav:ARC_TOKEN=value:sec-01gg4hcd881t1nncr04nmbeg04:arc_token
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

DATA(sbr://1323178467)

PEERDIR(
    yt/yt/tests/library
    contrib/python/pytest-benchmark
)

TEST_SRCS(
    test_yson_performance.py
)

REQUIREMENTS(
    ram:32
    cpu:2
    ram_disk:4
)

END()
