GO_LIBRARY()

SRCS(
    model.go
    service.go
)

PEERDIR(
    yt/yt/go/gpuagent/internal/pb
)

END()

RECURSE(
    nv
)
