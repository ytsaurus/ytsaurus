GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    component.go
    grpclog.go
    logger.go
    loggerv2.go
)

END()

RECURSE(
    glogger
    internal
    # yo
)
