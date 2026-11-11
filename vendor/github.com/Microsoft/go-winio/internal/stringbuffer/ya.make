GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

SRCS(
    wstring.go
)

IF (OS_WINDOWS)
    GO_TEST_SRCS(wstring_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
