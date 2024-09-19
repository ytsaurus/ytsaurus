GO_LIBRARY()

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestKeyTemplateProto)

SRCS(
    constant.go
    testutil.go
    wycheproofutil.go
)

GO_XTEST_SRCS(
    testutil_test.go
    wycheproofutil_test.go
)

END()

RECURSE(
    gotest
    hybrid
)
