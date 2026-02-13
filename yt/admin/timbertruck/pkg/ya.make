IF (NOT OPENSOURCE)
    RECURSE(
        antisecret
        logbroker
    )
ENDIF()

RECURSE(
    app
    pipelines
    timbertruck
    ttlog
    uploader
    ytlog
    ytqueue
    zstdsync
)
