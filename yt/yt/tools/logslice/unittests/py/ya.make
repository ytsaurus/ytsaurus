PY3TEST()

TEST_SRCS(
    test_logslice_py.py
)

# The script under test is loaded from the arcadia source tree via
# yatest.common.source_path; DATA ships it into the test environment.
DATA(
    arcadia/yt/yt/tools/logslice/logslice.py
)

SIZE(SMALL)

END()
