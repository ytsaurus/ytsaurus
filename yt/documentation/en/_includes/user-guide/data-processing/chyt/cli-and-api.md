# CLI and Python API

You can use the CHYT CLI and the CHYT Python API to run your clique or send a query to CHYT from a program or the command line. They can be obtained as part of the `ytsaurus-client` package.

The command line utility accepts two environment variables:
- `<cluster_name>`: The {{product-name}} cluster.
- `<alias>`: The used clique.

For example, you can use the following command for Linux and macOS  to set environment variables and no longer pass the `--proxy <cluster_name>` and `--alias *ch_public` parameters to all subsequent calls:

```bash
export YT_PROXY=<cluster_name> YT_ALIAS=*ch_public
```

The process that launches the clique can sometimes be informally called a *launcher*.

## ytsaurus-client { #ytsaurus-client }

The main way to get started with {{product-name}} is to install the `ytsaurus-client` package. To learn more about this, see [Python Wrapper](../../../../api/python/start.md).

Installing `ytsaurus-client` using [PyPi](https://pypi.org/):

```bash
pip install ytsaurus-client
```

{% note info "Note" %}

By default, the `pip install` command installs the latest stable version of the package, whereas the latest CHYT features are often found in unstable versions of `ytsaurus-client`. Therefore, if the package is installed in order to use CHYT, you must specify the `--pre` key when installing via PyPi.

{% endnote %}


`yt` is distributed as part of the library. In order to use the CHYT features, you can run any command as `yt clickhouse [command]`.

For example, you can run your own clique from the command line as follows:

```bash
yt clickhouse start-clique --proxy <cluster_name> --instance-count 4 --alias '*my_little_clique' --spec '{pool=<my_pool>}'
```

Besides that, the library contains the Python API for working with CHYT available in the `yt.clickhouse` module:

```bash
python3
Python 3.7.3 (default, Oct  7 2019, 12:56:13)
[GCC 8.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import yt.clickhouse as chyt
>>> import yt.wrapper as yt
>>> client = yt.YtClient("<cluster_name>")
>>> list(chyt.execute("select * from `//home/user/sample_table`", alias="*ch_public", client=client))
[{'a': 100}, {'a': 101}, {'a': 102}, {'a': 205}, {'a': 206}, {'a': 1100}]
```


----

For more information about the specification of the Python API and CLI features, see [Running a clique](../../../../user-guide/data-processing/chyt/cliques/start.md) and [Executing queries](../../../../user-guide/data-processing/chyt/reference/execute.md).
