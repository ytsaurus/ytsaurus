GO_LIBRARY()

PEERDIR(
    library/go/core/log/zap
    yt/yt/gpuagent/internal/agent
    yt/yt/gpuagent/internal/agent/nv
)

IF (NOT YT_CUSTOM_INTERNAL_BUILD)
    SRCS(providers_default.go)    
ELSE()
    SRCS(providers_custom.go)
    PEERDIR(
        yt/yt/gpuagent/internal/agent/mx
    )
ENDIF()

END()
