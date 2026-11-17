GO_LIBRARY()

SRCS(
    authentication.go
)

IF (NOT OPENSOURCE)
    SRCS(
        authentication_internal.go
    )

    GO_TEST_SRCS(authentication_internal_test.go)
ENDIF()

END()

RECURSE(
    gotest
)
