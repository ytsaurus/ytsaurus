GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.69.4)

SRCS(
    balancer.go
    conn_state_evaluator.go
    subconn.go
)

GO_TEST_SRCS(conn_state_evaluator_test.go)

END()

RECURSE(
    base
    endpointsharding
    gotest
    grpclb
    leastrequest
    pickfirst
    rls
    roundrobin
    weightedroundrobin
    weightedtarget
)
