RECURSE(
    app
    pipelines
    timbertruck
    ytlog
    ytqueue
)

IF (NOT OPENSOURCE)
    RECURSE(
        antisecret
        logbroker
    )
ENDIF()
