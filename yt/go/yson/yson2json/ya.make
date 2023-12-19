GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(raw_message.go)

GO_TEST_SRCS(raw_message_test.go)

END()

RECURSE(gotest)
