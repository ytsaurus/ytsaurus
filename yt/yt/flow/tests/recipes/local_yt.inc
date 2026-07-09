IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
    IF (NOT OPENSOURCE)
        DEFAULT(PREBUILT_YT_PACKAGE_DIR yt/packages/ytflow_latest)
    ELSE()
        SET(YT_RECIPE_BUILD_FROM_SOURCE yes)
    ENDIF()

    DEFAULT(YT_CONFIG_PATCH {rpc_proxy_count=1;node_count=3;scheduler_count=1;queue_agent_state_target_version=7;})

    INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)
ELSE()
    TAG(ya:not_autocheck)
ENDIF()
