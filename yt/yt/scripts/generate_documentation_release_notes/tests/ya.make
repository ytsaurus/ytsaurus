PY3TEST()

PEERDIR(
    contrib/python/requests
    library/python/testing/yatest_common
)

TEST_SRCS(
    test_release_notes.py
)

DATA(
    arcadia/yt/yt/scripts/generate_documentation_release_notes/__main__.py
)

END()
