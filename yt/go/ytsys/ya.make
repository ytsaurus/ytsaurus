GO_LIBRARY()

SRCS(
    client.go
    data_integrity.go
    types.go
)

GO_TEST_SRCS(
    data_integrity_test.go
    types_test.go
)

IF (NOT OPENSOURCE)
    GO_TEST_SRCS(client_test.go)
ENDIF()

END()

RECURSE(gotest)
