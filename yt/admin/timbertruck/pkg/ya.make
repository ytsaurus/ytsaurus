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
    uploader
    ytlog
    ytqueue
)
