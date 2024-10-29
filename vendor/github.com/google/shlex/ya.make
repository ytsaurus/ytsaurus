GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.0.0-20191202100458-e7afc7fbc510)

SRCS(
    shlex.go
)

GO_TEST_SRCS(shlex_test.go)

END()

RECURSE(
    gotest
)
