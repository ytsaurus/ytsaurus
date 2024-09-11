Recipe to deploy several YT connected replicas for tests.

Recipe uses prebuilt binaries from `yt/packages/latest` by default, but can compile from trunk if `YT_RECIPE_BUILD_FROM_SOURCE` is set.

Basic chaos configuration is supported: enabled by setting `YT_DB_MODE="chaos"`.

In case of chaos separate tablet_cell_bundle must be used instead of `default`, because it has special clock configuration which is necessary for chaos. This is set by non-empty YT_TABLET_CELL_BUNDLE_NAME variable. For non-chaos scenarios `default` bundle may be used as well.
