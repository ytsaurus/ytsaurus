PEERDIR(
   yt/yt/tests/integration
   yt/yt/tests/conftest_lib
   yt/python/contrib/python-six
)

DEPENDS(
    yt/yt/tools/yt_sudo_fixup
)

DEPENDS(yt/yt/packages/tests_package)

PY_SRCS(
    # This is a workaround for weird Arcadia Python issue. If we fail to specify separate namespace for the
    # test file from the original YT integration tests, packaging system would put test_journals.py
    # into module yt.yt.<...> resulting in module yt being overridden (!) resulting in lots of import errors
    # from modele yt (in particular, yt.packages.six).
    NAMESPACE original_tests
    yt/yt/tests/integration/tests/test_journals.py
)

TEST_SRCS(
    test_journals.py
    test_compatibility.py
)
