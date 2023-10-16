GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(new.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
