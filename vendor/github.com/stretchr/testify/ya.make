GO_LIBRARY()

LICENSE(MIT)

OWNER(g:go-contrib)

SRCS(doc.go)

GO_TEST_SRCS(package_test.go)

END()

RECURSE(
    assert
    gotest
    http
    mock
    require
    #suite
)
