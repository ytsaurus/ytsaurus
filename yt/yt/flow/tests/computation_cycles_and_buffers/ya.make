PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/computation_cycles_and_buffers/lib/ya.make.inc)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

END()

RECURSE(
    pipeline
)
