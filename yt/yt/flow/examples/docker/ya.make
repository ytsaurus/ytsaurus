UNION()

FILES(
    Dockerfile
    README.md
    controller.yson
    docker-compose.yml
    flow-start.sh
    package-noop.json
    pipeline.yson
    targets/flow_server.json
    worker.yson
    yt-sync-start.sh
)

END()

# The tests rely on library/recipes and Sandbox, both unavailable in opensource.
IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
