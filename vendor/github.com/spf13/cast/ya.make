GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.7.1)

SRCS(
    cast.go
    caste.go
    timeformattype_string.go
)

GO_TEST_SRCS(cast_test.go)

END()

RECURSE(
    gotest
)
