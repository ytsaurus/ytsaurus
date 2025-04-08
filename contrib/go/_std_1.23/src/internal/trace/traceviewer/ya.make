SUBSCRIBER(g:contrib)

GO_LIBRARY()
IF (TRUE)
    SRCS(
        emitter.go
        histogram.go
        http.go
        mmu.go
        pprof.go
    )
    
    GO_EMBED_PATTERN(static/trace_viewer_full.html)
GO_EMBED_PATTERN(static/webcomponents.min.js)
ENDIF()
END()
