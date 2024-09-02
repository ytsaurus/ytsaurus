GO_LIBRARY()

SRCS(
    datastore.go
    fs_watcher.go
    timbertruck.go
)

GO_TEST_SRCS(
    datastore_test.go
    fs_watcher_test.go
)

GO_XTEST_SRCS(timbertruck_test.go)

END()

RECURSE(
    gotest
)
