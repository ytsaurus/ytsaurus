GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.71.0)

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
    lazy
    leastrequest
    pickfirst
    rls
    roundrobin
    weightedroundrobin
    weightedtarget
)
