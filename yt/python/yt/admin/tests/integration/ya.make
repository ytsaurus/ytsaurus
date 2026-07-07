PY3TEST()

TEST_SRCS(
    test_remove_master_unrecognized_options.py
)

PEERDIR(
    yt/python/client_with_admin
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTests.txt)

END()
