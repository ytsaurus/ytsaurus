UNION()

FILES(
    README.md
    controller.yson
    docker-compose.yml
    pipeline.yson
    targets/flow_server.json
    worker.yson
    worker2.yson
    yt_sync.py
)

END()

# The tests rely on library/recipes and Sandbox, both unavailable in opensource.
IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        tests
    )
ENDIF()
