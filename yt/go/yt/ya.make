GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    acl.go
    batch_request.go
    batch.go
    config.go
    credentials.go
    discovery_client.go
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

IF (NOT OPENSOURCE)
    SRCS(
        endpoint_set_discovery.go
    )
ENDIF()

GO_TEST_SRCS(
    config_test.go
    example_test.go
)

END()

RECURSE(
    clienttest
    gotest
    integration
    internal
    multicell
    ytdiscovery
    ythttp
    ytjaeger
    ytrpc
)

IF (NOT OPENSOURCE)
    RECURSE(
        yttvm
    )
ENDIF()
