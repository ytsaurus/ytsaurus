GO_PROGRAM()

SRCS(
    main.go
    url_download_function.go
)

GO_TEST_SRCS(
    url_download_function_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
    test
)
