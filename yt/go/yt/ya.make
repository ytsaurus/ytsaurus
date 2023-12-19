GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    acl.go
    batch.go
    config.go
    credentials.go
    interface.go
    mount.go
    query_tracker.go
    range_operations.go
    skynet.go
    tables.go
    tx.go
    types.go
    webui_url.go
    writer.go
)

GO_TEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    integration
    internal
    ythttp
    ytjaeger
    ytrpc
)

IF (NOT OPENSOURCE)
    RECURSE(
        yttvm
    )
ENDIF()
