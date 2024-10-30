GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.7.0)

SRCS(
    aes_siv.go
)

GO_XTEST_SRCS(aes_siv_test.go)

END()

RECURSE(
    gotest
)
