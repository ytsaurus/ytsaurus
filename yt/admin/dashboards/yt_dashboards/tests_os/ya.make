PY3TEST()

STYLE_PYTHON()

IF (NOT OPENSOURCE)
    SKIP_TEST(This test only runs with -DOPENSOURCE=true; canondata for it is generated in the opensource build.)
ENDIF()

TEST_SRCS(
    test_dashboards.py
)

PEERDIR(
    yt/admin/dashboards
    yt/admin/dashboards/yt_dashboards/testlib
)

END()
