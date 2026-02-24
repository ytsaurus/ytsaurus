GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.49.0)

SRCS(
    ascii.go
    ciphers.go
    client_conn_pool.go
    config.go
    config_go125.go
    databuffer.go
    errors.go
    flow.go
    frame.go
    gotrack.go
    http2.go
    pipe.go
    server.go
    transport.go
    unencrypted.go
    write.go
    writesched.go
    writesched_priority_rfc7540.go
    writesched_priority_rfc9218.go
    writesched_random.go
    writesched_roundrobin.go
)

END()

RECURSE(
    h2c
    h2i
    hpack
)
