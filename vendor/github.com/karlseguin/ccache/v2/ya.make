GO_LIBRARY()

LICENSE(MIT)

VERSION(v2.0.8)

SRCS(
    bucket.go
    cache.go
    configuration.go
    item.go
    layeredbucket.go
    layeredcache.go
    secondarycache.go
)

GO_TEST_SRCS(
    bucket_test.go
    cache_test.go
    configuration_test.go
    item_test.go
    layeredcache_test.go
    secondarycache_test.go
)

END()

RECURSE(
    gotest
)
