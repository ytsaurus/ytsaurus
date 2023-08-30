# Executing queries

You can use this command to execute a random query in the clique. Also see the [How to try](../../../../../user-guide/data-processing/chyt/try-chyt.md) article which lists alternative ways to query a CHYT clique.

## CLI { #cli }

The `yt clickhouse execute` command enables the following parameters (default values are specified in brackets):

- `query`: The query.

- `--proxy`: The cluster on which the clique will be started, for example. This option can also be specified via the `YT_PROXY` environment variable (for example, `export YT_PROXY=<cluster_name>`).

- `--alias`: The alias under which the clique will be available. It should be a string starting with an asterisk. This option can also be specified via the `CHYT_ALIAS` environment variable (for example, `export CHYT_ALIAS=*example`).

- `--format` [TabSeparated]: The [ClickHouse format](https://clickhouse.tech/docs/ru/interfaces/formats/) in which to output the result.

- `--setting`: Enables you to pass a random setting to the query in `<key>=<value>` format, can be specified several times.

## Python API   { #python }

{% note warning "Attention!" %}

To use this function , be **sure to** configure logging at the **Debug** level. It is impossible to answer any question about query execution (including "why does it run so long", "why did it fail", "why did it output such responses") without debug logs.

{% endnote %}

The following method (some of the parameters are intentionally not mentioned because they are not intended to be used in a usual situation) is available in the `yt.clickhouse` module:

```python
def execute(
    query,
    alias=None,
    format=None,
    settings=None,
    raw=None,
    client=None):
    pass
```

Some of the parameters overlap with the command line options. The same is true for them as in the previous section. The function returns a string generator from the result and the strings can be python objects (lists, dictionaries, numbers, and strings) and strings in ordered format.

Let's describe the individual parameters and the differences from starting via the CLI:

- `format` (None) and `raw` (False): These options control the type of returned values. If `raw = False`, the function will turn values into python objects similarly to how the {{product-name}} Python API read_table method does it. If `raw = True`, you have to additionally specify the ClickHouse format. Then the returned value will be a string iterator (str) in ordered format.

- `settings` ({}): A dict with additional ClickHouse [settings](https://clickhouse.tech/docs/ru/operations/settings/settings/) for the query.

- `client`: A client from the {{product-name}} cluster in which the query to the clique should be run. The global client is taken by default. You can, for example, pass `yt.wrapper.YtClient("<cluster_name>")` as the client and thus specify the desired cluster. Learn more in the relevant section of the Python API documentation.
