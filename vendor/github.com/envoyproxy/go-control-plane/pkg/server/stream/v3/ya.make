GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.5-0.20251024222203-75eaa193e329)

SRCS(
    stream.go
    subscription.go
)

GO_TEST_SRCS(subscription_test.go)

END()

RECURSE(
    gotest
)
