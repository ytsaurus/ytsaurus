GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(blob_table.go)

GO_TEST_SRCS(
    blob_table_test.go
    example_test.go
)

END()

IF (NOT OPENSOURCE)
    RECURSE(gotest)
ENDIF()
