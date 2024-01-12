В составе распространяется программа `yt`. Для того чтобы воспользоваться функциональностью CHYT, можно запустить любую команду как `yt clickhouse [command]`.

Например, следующим образом можно сделать тестовый запрос в публичную клику `ch_public`:

```bash
yt clickhouse execute "select 'Hello world'" --alias *ch_public
```

Помимо этого пакет содержит Python API для работы с CHYT, доступный в модуле `yt.clickhouse`:

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

Библиотека доступна для Python 3.

Детальную спецификацию возможностей Python API и CLI вы можете найти в статье [Выполнение запросов](../../../../user-guide/data-processing/chyt/reference/execute.md).
