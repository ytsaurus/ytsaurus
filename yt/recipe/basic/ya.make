PY3_PROGRAM(yt_recipe)

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/recipe/basic/cluster_factory
    yt/recipe/basic/lib
    library/python/testing/recipe
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        test_job_log_viewer
        test_query_tracker
    )
ENDIF()
