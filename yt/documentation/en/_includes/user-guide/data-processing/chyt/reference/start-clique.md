# Starting a clique

Use this command to start your clique on any cluster. A detailed tutorial on how to start your own clique is available in the [How to start a private clique](../../../../../user-guide/data-processing/chyt/cliques/start.md) article.

## CLI { #cli }

The `yt clickhouse start-clique` command enables the following parameters (default values are specified in brackets):

- `--instance-count`: The number of instances in the clique (server processes).

- `--proxy`: The cluster on which the clique will be started. This option can also be specified via the `YT_PROXY` environment variable (for example, `export YT_PROXY=<cluster_name>`).

- `--alias` The alias under which the clique will be available. It should be a string starting with `*`. This option can also be specified via the `CHYT_ALIAS` environment variable (for example, `export CHYT_ALIAS=*example`).

- `--cpu-limit` [15]: The number of cores available to each instance.

- `--memory`: The configuration of RAM available to each instance. All values must be specified in bytes.

   - `clickhouse` [16 GB]: The amount of memory reserved for queries in ClickHouse.

   - `reader` [12 GB]: The amount of memory allocated for internal read needs.

   - `uncompressed_block_cache` [16 GB]: The size of cache of uncompressed data blocks.

   - `compressed_block_cache` (available starting with version 2.05) [0 B]: The size of cache of compressed data blocks.

   - `chunk_meta_cache` (available starting with version 2.05) [1 GB]: The size of chunk metainformation cache.

- `--abort-existing`: Automatically abort the running clique with the same alias.

- `--spec`: Use this option to add random parameters to the Vanilla {{product-name}} operation specification. It may be useful to specify these values when starting CHYT:

   - `pool`: The computing pool in which the clique should be started. If the specified pool is missing, the clique will be started in the user's personal pool that has no guarantees, which is not recommended.

   - `acl`: A list of users and groups with the permission to run queries in this clique. For more information, see [Operating a private clique](../../../../../user-guide/data-processing/chyt/cliques/administration.md#access).

   - `title`: The human-readable name for the clique, visible in the [clique interface](../cliques/ui.md).

   - `preemption_mode` [`normal`]: Using this option, you can enable [graceful preemption](../../../../../user-guide/data-processing/chyt/cliques/resources.md#preemption-dif) mode if you specify the `graceful` value.

- `--cypress-base-config-path` (`//sys/clickhouse/config`): The path to the base configuration of the corresponding cluster in Cypress.

- `--cypress-geodata-path` (`//sys/clickhouse/geodata/geodata.tgz`): Use geodata located on this path in the clique. Prepared geodata is located at `//sys/clickhouse/geodata`. There is a link `geodata.tgz` to the most recent version, i.e. the most recent archive with geodata is taken by default.

- `--clickhouse-config`: The additional patch to the base configuration in the form of a YSON dict. See the [Instance configuration](../../../../../user-guide/data-processing/chyt/reference/configuration.md) section.

## Python API { #python }

The following method (some of the parameters are intentionally not mentioned because they are not intended to be used in a usual situation) is available in the `yt.clickhouse` module:

```python
def start_clique(instance_count,
                 alias=None,
                 clickhouse_config=None,
                 cpu_limit=None,
                 memory_config=None,
                 enable_monitoring=None,
                 cypress_geodata_path=None,
                 description=None,
                 abort_existing=None,
                 spec=None,
                 client=None,
                 wait_for_instances=None):
    pass
```

Some of the parameters overlap with the command line options. The same is true for them as in the previous section. Individual parameters:

- `memory_footprint` (20 GB): The additional amount of RAM for the {{product-name}} runtime library.

- `wait_for_instances` (True): If this option is set to True, the startup function waits until all clique instances are ready and only then gives control.

- `client`: A client from the {{product-name}} cluster in which the clique should be started.  The global client is taken by default: you can, for example, pass `yt.wrapper.YtClient("<cluster_name>")` as the client and thus specify the desired cluster.
