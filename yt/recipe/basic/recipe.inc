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
DEFAULT(QT_CONFIG_PATCH {})
DEFAULT(YT_CLUSTER_NAMES primary)
DEFAULT(YT_CLUSTER_CONFIG_PATCHES {})
DEFAULT(YT_WITH_QUERY_TRACKER no)
# yqla resource, built by yt/yql/package/build_local_yt_yqla.sh (adcd0a538e7a4c601d802df738d28dfc53bb38ca)
DEFAULT(YQLA_RESOURCE sbr://14129260865)
DEFAULT(YT_REMOVE_RUNTIME_DATA no)

SET(YT_RECIPE_STOP_ARGS)
IF (YT_REMOVE_RUNTIME_DATA == yes)
    SET_APPEND(YT_RECIPE_STOP_ARGS --remove-runtime-data)
ENDIF()

IF (YT_WITH_QUERY_TRACKER == yes)
    DATA(${YQLA_RESOURCE})
    SET(QT_CONFIG_PATCH {components=[{name=query-tracker};{name=yql-agent}]})
ENDIF()

USE_RECIPE(
    yt/recipe/basic/yt_recipe
    --cluster-names ${YT_CLUSTER_NAMES}
    --cluster-config-patches ${YT_CLUSTER_CONFIG_PATCHES}
    --config-patch \"${QT_CONFIG_PATCH}\"
    --config-patch \"${YT_CONFIG_PATCH}\"
    --cleanup-working-directory
    --package-dir ${YT_PACKAGE_DIR}
    ${YT_RECIPE_STOP_ARGS}
)
