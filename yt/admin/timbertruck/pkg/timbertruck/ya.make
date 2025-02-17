GO_LIBRARY()

SRCS(
    datastore.go
    fs_watcher.go
    inotify_watcher.go
    timbertruck.go
    timbertruck_metrics.go
)

GO_TEST_SRCS(
    datastore_migrations_test.go
    datastore_test.go
    fs_watcher_test.go
    timbertruck_internal_test.go
)

GO_XTEST_SRCS(timbertruck_test.go)

GO_TEST_EMBED_PATTERN(version1.txt)

END()

RECURSE(
    gotest
)
