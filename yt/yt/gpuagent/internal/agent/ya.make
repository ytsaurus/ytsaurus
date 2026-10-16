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
    factory
    nv
)

IF (YT_CUSTOM_INTERNAL_BUILD)
    RECURSE(
        mx
    )
ENDIF()
