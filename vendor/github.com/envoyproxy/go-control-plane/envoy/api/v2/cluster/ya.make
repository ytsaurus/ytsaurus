GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.32.4)

SRCS(
    circuit_breaker.pb.go
    circuit_breaker.pb.validate.go
    filter.pb.go
    filter.pb.validate.go
    outlier_detection.pb.go
    outlier_detection.pb.validate.go
)

END()
