The package includes `yt`. In order to use the CHYT features, you can run any command as `yt clickhouse [command]`.

For example, you can make a test query to the public clique `ch_public` as follows:

```bash
yt clickhouse execute "select 'Hello world'" --alias *ch_public
```

The package additionally contains the CHYT Python API available in the `yt.clickhouse` module:

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

The library is available for Python 3.

For more detailed specification of the Python API and CLI features, see [Executing queries](../../../../user-guide/data-processing/chyt/reference/execute.md).
