RECURSE(
    agent
    api
    app
    auth
    chyt
    discovery
    httpserver
    jupyt
    monitoring
    sleep
    strawberry
)

IF (NOT OPENSOURCE)
    RECURSE(
        dq
    )
ENDIF()
