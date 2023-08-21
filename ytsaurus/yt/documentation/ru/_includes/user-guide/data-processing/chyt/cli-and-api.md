# CLI и Python API

Чтобы запустить свою клику или отправить запрос в CHYT из программы либо из командной строки, можно воспользоваться CHYT CLI и CHYT Python API. Они могут быть получены в составе пакета `ytsaurus-client`.

Утилита командной строки воспринимает две переменные окружения:
- `<cluster_name>` – кластер {{product-name}};
- `<alias>` – используемая клика.

Например, под Linux и macOS можно использовать следующую команду, чтобы установить переменные окружения и больше не передавать параметры `--proxy <cluster_name>` и `--alias *ch_public` во все последующие вызовы:

```bash
export YT_PROXY=<cluster_name> YT_ALIAS=*ch_public
```

Запускающий клику процесс иногда может неформально называться *лончером*.

## ytsaurus-client { #ytsaurus-client }

Основной способ начать работать с {{product-name}} — установить пакет `ytsaurus-client`. Подробнее об этом можно прочитать в разделе [Python Wrapper](../../../../api/python/start.md).

Установка `ytsaurus-client` с помощью [PyPi](https://pypi.org/):

```bash
pip install ytsaurus-client
```

{% note info "Примечание" %}

Команда `pip install` по умолчанию ставит самую свежую стабильную версию пакета, тогда как самая свежая функциональность CHYT зачастую находится в нестабильных версиях `ytsaurus-client`. Поэтому если пакет ставится с целью использования CHYT, нужно указать ключ `--pre` при установке через PyPi.

{% endnote %}

Доступно для Python 3. В составе распространяется программа `yt`. Для того, чтобы воспользоваться функциональностью CHYT, можно запустить любую команду как `yt clickhouse [command]`.

Например, следующим образом можно сделать тестовый запрос в публичную клику `ch_public`:

```bash
yt clickhouse execute 'select "Hello world"' --alias *ch_public
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

Детальную спецификацию возможностей Python API и CLI вы можете найти в статьях [Запуск клики](../../../../user-guide/data-processing/chyt/cliques/start.md) и [Выполнение запросов](../../../../user-guide/data-processing/chyt/reference/execute.md).


