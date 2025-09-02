GO_LIBRARY()

SRCS(
    dummy.go
    model.go
    service.go
)

PEERDIR(
    yt/yt/gpuagent/internal/pb
)

END()

RECURSE(
    nv
)
