GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.36.0)

SRCS(
    doc.go
    text_map_carrier.go
    text_map_propagator.go
)

GO_TEST_SRCS(
    text_map_carrier_test.go
    text_map_propagator_test.go
)

END()

RECURSE(
    gotest
)
