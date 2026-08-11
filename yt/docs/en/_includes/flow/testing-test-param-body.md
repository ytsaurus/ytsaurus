## Test launch parameters {#test-param}

You configure the behavior of integration tests using `--test-param NAME=VAL`:

#|
|| **Parameter** | **Default** | **Values** | **Action** ||
|| `RUNNER_LOG_LEVEL` | | `Error`, `Info`, `Debug`, … | Sets the logging level for the process that runs the pipeline (runner). ||
|| `PAUSE_BEFORE_FLOW_PROCESS_FEDERATION_TEARDOWN` | `0` | `0`, `1` | Pauses the test before stopping Flow processes. Combined with `--test-disable-timeout`, this lets you keep the local {{product-name}} and the Flow process federation running for a long time so you can study them at your own pace via the UI. ||
|| `EXTERNAL_YT_CONFIG` | (not set) | YSON — see below | Runs the pipeline on real external {{product-name}} clusters instead of the local recipe. ||
|#

Examples:

```bash
ya make -A --test-param RUNNER_LOG_LEVEL=Debug
ya make -A --test-disable-timeout --test-param PAUSE_BEFORE_FLOW_PROCESS_FEDERATION_TEARDOWN=1
```

### `EXTERNAL_YT_CONFIG` {#external-yt-config}

The local {{product-name}} still starts via the recipe, but the test ignores it.

Required fields that are common for all clusters:

- `path` — base directory.
- `tablet_cell_bundle` — bundle for the dynamic tables that are created.
- `proxy_role` — RPC proxy role.

Optional: `primary_medium` (default `"default"`).

The `clusters` list — the first element is primary. Record fields: `cluster_name` (required), `proxy_url` (defaults to `cluster_name`).

Authorization: when using an external {{product-name}}, `YT_TOKEN`/`YT_USER` from the local recipe are cleared; yt-wrapper picks up the token from `~/.yt/token`.

Isolation: `work_yt_path = path/<local-username>/<test_name>`; the `path/<username>` directory is deleted and recreated once per class in `setup_class`.

Example:

```bash
ya make -A --test-param 'EXTERNAL_YT_CONFIG={path="//tmp/yt_flow";tablet_cell_bundle=default;proxy_role=default;clusters=[{cluster_name={{production-cluster}}};];}'
```