PY3TEST()

SIZE(SMALL)

PEERDIR(
    library/cpp/xdelta3/py_bindings
)

TEST_SRCS(
    test.py
)


NO_CHECK_IMPORTS()

END()
