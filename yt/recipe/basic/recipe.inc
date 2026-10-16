DEPENDS(
    yt/recipe/basic
)

DEFAULT(PREBUILT_YT_PACKAGE_DIR yt/packages/latest)

IF (NOT YT_RECIPE_BUILD_FROM_SOURCE)
    DEPENDS(
        ${PREBUILT_YT_PACKAGE_DIR}
    )
    SET(YT_PACKAGE_DIR "${PREBUILT_YT_PACKAGE_DIR}")
ELSE()
    DEPENDS(
        yt/yt/packages/tests_package
    )
    SET(YT_PACKAGE_DIR "yt/yt/packages/tests_package")
ENDIF()

DEFAULT(YT_CONFIG_PATCH {})
DEFAULT(YT_CLUSTER_NAMES primary)
DEFAULT(YT_CLUSTER_CONFIG_PATCHES {})
DEFAULT(YT_WITH_QUERY_TRACKER no)
DEFAULT(YT_REMOVE_RUNTIME_DATA no)

SET(YT_RECIPE_STOP_ARGS)
IF (YT_REMOVE_RUNTIME_DATA == yes)
    SET_APPEND(YT_RECIPE_STOP_ARGS --remove-runtime-data)
ENDIF()

IF (YT_WITH_QUERY_TRACKER == yes)
    # yqla resource, built by yt/yql/package/build_local_yt_yqla.sh
    DATA(sbr://8940898069)
    USE_RECIPE(
        yt/recipe/basic/yt_recipe
        --cluster-names ${YT_CLUSTER_NAMES}
        --cluster-config-patches ${YT_CLUSTER_CONFIG_PATCHES}
        --config-patch \"${YT_CONFIG_PATCH}\"
        --cleanup-working-directory
        --package-dir ${YT_PACKAGE_DIR}
        --with-query-tracker
        ${YT_RECIPE_STOP_ARGS}
    )
ELSE()
    USE_RECIPE(
        yt/recipe/basic/yt_recipe
        --cluster-names ${YT_CLUSTER_NAMES}
        --cluster-config-patches ${YT_CLUSTER_CONFIG_PATCHES}
        --config-patch \"${YT_CONFIG_PATCH}\"
        --cleanup-working-directory
        --package-dir ${YT_PACKAGE_DIR}
        ${YT_RECIPE_STOP_ARGS}
    )
ENDIF()
