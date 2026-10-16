PY3TEST()

TEST_SRCS(
    test_logslice_cli.py
)

DEPENDS(
    yt/yt/tools/logslice/bin
)

DATA(
    arcadia/yt/yt/tools/logslice/unittests/cli/fixtures/local_cli.log
)

SIZE(SMALL)

END()
