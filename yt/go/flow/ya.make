GO_LIBRARY()

SRCS(
    computation.go
    context.go
    cpu.go
    flow.go
    job.go
    message.go
    memory.go
    memory_marker.go
    output.go
    protomap.go
    payload.go
    server.go
    state.go
    stream.go
)

GO_TEST_SRCS(
    computation_test.go
    context_test.go
    cpu_test.go
    flow_test.go
    job_test.go
    message_test.go
    memory_test.go
    output_test.go
    protomap_test.go
    payload_test.go
    server_test.go
    state_test.go
    stream_test.go
)

END()

RECURSE(
    flowtest
    gotest
    runner
)
