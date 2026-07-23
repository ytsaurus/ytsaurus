GO_LIBRARY()

LICENSE(MIT)

VERSION(v4.13.4)

SRCS(
    bind.go
    binder.go
    context.go
    context_fs.go
    echo.go
    echo_fs.go
    group.go
    group_fs.go
    ip.go
    json.go
    log.go
    renderer.go
    response.go
    router.go
)

GO_TEST_SRCS(
    # bind_test.go
    # binder_test.go
    # context_fs_test.go
    # context_test.go
    # echo_fs_test.go
    # echo_test.go
    # group_fs_test.go
    # group_test.go
    # ip_test.go
    # json_test.go
    # renderer_test.go
    # response_test.go
    # router_test.go
)

GO_XTEST_SRCS(binder_external_test.go)

END()

RECURSE(
    gotest
    middleware
)
