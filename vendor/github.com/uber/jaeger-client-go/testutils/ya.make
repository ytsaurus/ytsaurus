GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.30.0+incompatible)

SRCS(
    mock_agent.go
    sampling_manager.go
    udp_transport.go
)

GO_TEST_SRCS(
    mock_agent_test.go
    udp_transport_test.go
)

END()

RECURSE(
    gotest
)
