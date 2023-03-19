# Как попробовать

Попробовать CHYT можно несколькими способами.

Самый простой способ — запустить запрос через веб-интерфейс YQL. В качестве клики можно взять публичную клику c алиасом `*ch_publiс`. Это главная общедоступная клика, есть на каждом кластере присутствия CHYT.

{% note warning "Внимание" %}

К публичной клике есть доступ у всех пользователей {{product-name}}, поэтому есть риск, что она может быть недоступна из-за запросов других пользователей или занята чужими расчетами. В связи с этим не надо основывать важные процессы и дашборды на публичной клике. Публичная клика по сути является демо-версией CHYT. Для своей команды настоятельно рекомендуется поднимать отдельную клику.

{% endnote %}

Для запуска запроса нужно:

1. Перейти на страницу YQL в кластере.

2. Отправить запрос, который начинается с `USE chyt.<cluster_name>[/<alias>];`, где:
  - `<cluster_name>` — имя кластера {{product-name}};
  - `<alias>` — алиас клики без символа `*`, если надо указать конкретную клику.

  Например: `USE chyt.yt_cluster/my_clique_alias`

{% note info "Примечание" %}

В случае публичной клики `/ch_public` название можно опустить. Таким образом, запись `USE chyt.yt_cluster;` эквивалента `USE chyt.yt_cluster/ch_public;`.

{% endnote %}

## HTTP-интерфейс { #http }

Доступ к клике осуществляется через тяжелые HTTP-прокси {{product-name}}. Самый удобный способ обратиться к клике — отправить запрос на имя соответствующего кластера и обработать перенаправление (redirect). В случае утилиты curl это означает, что необходимо указать флаг `--location-trusted`.

Ниже смотрите пример, как задать с помощью утилиты curl командной строки запрос к клике, зная имя кластера (`YT_PROXY`), токен доступа к {{product-name}} (`YT_TOKEN`) и алиас клики (`CHYT_ALIAS`).

```bash
curl --location-trusted -H "Authorization: OAuth $YT_TOKEN" "$YT_PROXY/query?database=$CHYT_ALIAS" -d 'SELECT Avg(a) FROM "//sys/clickhouse/sample_table"'
224.30769230769232
```

## ODBC-интерфейс { #odbc }

CHYT можно пользоваться из Tableau посредством [Clickhouse ODBC Driver](https://github.com/ClickHouse/clickhouse-odbc).

Для этого достаточно составить ODBC-конфигурацию, представленную ниже. Переменная `YT_TOKEN` обозначает токен доступа к {{product-name}} (`AQAD-qJ…`), `CHYT_ALIAS` — алиас клики (`*ch_public`), `YT_PROXY` — имя кластера.

```bash
[ClickHouse]
Driver = <path to driver libclickhouseodbc>
# For example /usr/local/opt/clickhouse-odbc/lib/libclickhouseodbc.dylib
user = default
password = $YT_TOKEN
database = $CHYT_ALIAS
url = http://$YT_PROXY/query
port = 80
# trace = 1
# TraceFile=/tmp/odbc.log
```

Ниже представлен пример, как обратиться к CHYT через ODBC-интерфейс из командной строки, используя утилиту [isql](https://en.wikipedia.org/wiki/UnixODBC). Для этого необходимо записать файл с конфигурацией `~/.odbc.ini`, поставить Clickhouse ODBC драйвер из официального репозитория и воспользоваться утилитой isql.

```bash
isql -v clickhouse
+---------------------------------------+
| Connected!                            |
|                                       |
| sql-statement                         |
| help [tablename]                      |
| quit                                  |
|                                       |
+---------------------------------------+
SQL> SELECT * FROM "//tmp/sample_table"
+--+
| a|
+--+
| 2|
| 9|
| 6|
| 4|
| 8|
| 3|
| 1|
| 5|
| 7|
| 0|
+--+
SQLRowCount returns 10
10 rows fetched
```

## JDBC-интерфейс { #jdbc }

JDBC-драйвер конфигурируется схожим образом с ODBC-драйвером, смотрите пример ниже. Переменная `YT_TOKEN` обозначает токен доступа к {{product-name}} (`AQAD-qJ…`),  `CHYT_ALIAS` — идентификатор либо алиас клики (`*ch_public`), `YT_PROXY` — имя кластера.


```bash
JDBC URL = jdbc:clickhouse://{host}:{port}/query?database={database}
host = $YT_PROXY
user = default
password = $YT_TOKEN
database = $CHYT_ALIAS
port = 80
```

Кроме того, необходимо изменить свойства драйвера:

- `check_for_redirects=true`;
- `use_path_as_db=false`.

JDBC-драйвер можно использовать как основу при использовании open-source программы для работы с базами данных [DBeaver](https://dbeaver.io). Тестирование проводилось на версии DBeaver 6.1.5. В случае если ClickHouse-JDBC драйвер не устанавливается автоматически, его необходимо [скачать](https://github.com/yandex/clickhouse-jdbc) вручную. При этом необходимо использовать версию драйвера старше 0.1.55.

## CLI и Python API { #cli-and-api }

У CHYT есть официальный API для языка Python и официальная утилита командной строки. Подробнее можно прочитать [CLI и Python API](../../../../user-guide/data-processing/chyt/cli-and-api.md).







