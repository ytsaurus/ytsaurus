GO_LIBRARY()

SRCS(
    cluster.go
    models.go
    resource_usage.go
    table.go
)

GO_TEST_SRCS(
    cluster_test.go
    table_test.go
)


END()

RECURSE(
    gotest
)
