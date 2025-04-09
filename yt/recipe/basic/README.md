Recipe to deploy several YT connected replicas for tests.

## ya.make file options

### `YT_CLUSTER_NAMES` (default: `primary`)

Comma separated list of cluster names (e.g. `first,second`). Recipe starts specified clusters. Their http proxies are available in `YT_PROXY_<cluster-name-upper-case>` environment variables (for default configuration it's `YT_PROXY_PRIMARY`)

### `YT_CONFIG_PATCH` (default `{}`)

Yson-map specifying additional options for `yt.local.start`, check [documentation](https://yt.yandex-team.ru/docs/other/local-mode#publichnyj-interfejs)

### `YT_CLUSTER_CONFIG_PATCHES` (default: `{}`)
Yson-map where key is cluster name and value is additional yson config for this cluster.

### `YT_RECIPE_BUILD_FROM_SOURCE` (default: `no`)

If set to `no` then recipe uses precompiled binaries from `yt/packages/latest`.
If set to `yes` then recipe uses binaries compiled from arcadia.

### `YT_DB_MODE` (default: `<empty>`)

If set to `chaos` then basic chaos configuration is performed. In this case separate tablet_cell_bundle must be used instead of `default`, because it has special clock configuration which is necessary for chaos. This is set by non-empty `YT_TABLET_CELL_BUNDLE_NAME` variable. For non-chaos scenarios `default` bundle may be used as well.
