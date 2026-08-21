PY3TEST()

TEST_SRCS(
    test_logslice_py.py
)

# The script under test is loaded from the arcadia source tree via
# yatest.common.source_path; DATA ships it into the test environment.
DATA(
    arcadia/yt/yt/tools/logslice/logslice.py
    arcadia/yt/yt/tools/logslice/unittests/py/fixtures/ytadmin_13061/README.md
    arcadia/yt/yt/tools/logslice/unittests/py/fixtures/ytadmin_13061/debug.log
    arcadia/yt/yt/tools/logslice/unittests/py/fixtures/ytadmin_13061/error.log
    arcadia/yt/yt/tools/logslice/unittests/py/fixtures/ytadmin_13061/info.log
    arcadia/yt/yt/tools/logslice/unittests/py/fixtures/ytadmin_13061/rotation_outcomes.json
    arcadia/yt/yt/tools/logslice/unittests/py/fixtures/ytadmin_58495/authentication_unavailable.json
)

SIZE(SMALL)

END()
