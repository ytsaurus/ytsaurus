GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.6.2)

IF (OS_WINDOWS)
    SRCS(
        hook.go
        opts.go
    )

    GO_TEST_SRCS(hook_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
