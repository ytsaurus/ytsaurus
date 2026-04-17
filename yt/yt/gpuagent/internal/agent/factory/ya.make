GO_LIBRARY()

PEERDIR(
    library/go/core/log/zap
    yt/yt/gpuagent/internal/agent
    yt/yt/gpuagent/internal/agent/nv
)

IF (OPENSOURCE)
    SRCS(providers_external.go)    
ELSE()
    SRCS(providers_internal.go)
    PEERDIR(
        yt/yt/gpuagent/internal/agent/mx
    )
ENDIF()

END()
