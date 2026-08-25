DEPENDS(
    yt/recipe/chaos
)

DEFAULT(YT_PACKAGE_DIR "yt/packages/latest")

IF (NOT YT_RECIPE_BUILD_FROM_SOURCE)
    DEPENDS(
        yt/packages/latest
    )
    SET(YT_PACKAGE_DIR "yt/packages/latest")
ELSE()
    DEPENDS(
        yt/yt/packages/tests_package
    )
    SET(YT_PACKAGE_DIR "yt/yt/packages/tests_package")
ENDIF()

DEFAULT(YT_CONFIG_PATCH {})
DEFAULT(YT_CLUSTER_NAMES primary)
DEFAULT(YT_CLUSTER_CONFIG_PATCHES {primary={}})

DEFAULT(YT_DB_MODE "replicated")
DEFAULT(YT_TABLET_CELL_BUNDLE_NAME "")

USE_RECIPE(
    yt/recipe/chaos/yt_recipe
    --cluster-names ${YT_CLUSTER_NAMES}
    --cluster-config-patches ${YT_CLUSTER_CONFIG_PATCHES}
    --default-config ${YT_DEFAULT_CONFIG}
    --config-patch ${YT_CONFIG_PATCH}
    --cleanup-working-directory
    --package-dir ${YT_PACKAGE_DIR}
    --db-mode ${YT_DB_MODE}
    --tablet-cell-bundle-name '${YT_TABLET_CELL_BUNDLE_NAME}'
)
