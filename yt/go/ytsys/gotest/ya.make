GO_TEST_FOR(yt/go/ytsys)

IF (NOT OPENSOURCE)
    SIZE(MEDIUM)

    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_queue_agents.inc)
ENDIF()

END()
