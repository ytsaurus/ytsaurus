PY23_TEST()

STYLE_PYTHON()
NO_BUILD_IF(SANITIZER_TYPE)

TEST_SRCS(
    test_confmerge.py
)

PEERDIR(
    library/python/confmerge
)

END()
