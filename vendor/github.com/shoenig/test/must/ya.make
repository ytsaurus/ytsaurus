GO_LIBRARY()

LICENSE(MPL-2.0)

VERSION(v0.6.6)

SRCS(
    assert.go
    invocations.go
    must.go
    scripts.go
    settings.go
)

GO_TEST_SRCS(
    assert_test.go
    invocations_test.go
    must_test.go
    scripts_test.go
    settings_test.go
)

IF (OS_LINUX)
    SRCS(
        fs_default.go
    )

    GO_TEST_SRCS(examples_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        fs_default.go
    )

    GO_TEST_SRCS(examples_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        fs_windows.go
    )
ENDIF()

END()

RECURSE(
    gotest
)
