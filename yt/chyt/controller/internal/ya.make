RECURSE(
    agent
    api
    app
    auth
    chyt
    discovery
    httpserver
    jupyt
    livy
    monitoring
    sleep
    strawberry
)

IF (NOT OPENSOURCE)
    RECURSE(
        dq
    )
ENDIF()
