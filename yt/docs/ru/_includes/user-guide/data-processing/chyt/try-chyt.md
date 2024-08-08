# Как попробовать

Попробовать CHYT можно несколькими способами.

Самый простой способ — запустить запрос через веб-интерфейс {% if audience == "internal" %}YQL{% else %}Query Tracker{% endif %}. В качестве клики можно взять публичную клику c алиасом `ch_public`. Это главная общедоступная клика, есть на каждом кластере присутствия CHYT.

{% note info "Примечание" %}

В версиях YT 24.1 и ранее алиас клики указывается со звёздочкой в начале, например, `*ch_public`. В текущей версии `*ch_public` и `ch_public` соответствуют одному алиасу.

{% endnote %}

{% note warning "Внимание" %}

К публичной клике есть доступ у всех пользователей {{product-name}}, поэтому есть риск, что она может быть недоступна из-за запросов других пользователей или занята чужими расчетами. В связи с этим не надо основывать важные процессы и дашборды на публичной клике. Публичная клика по сути является демо-версией CHYT. Для своей команды настоятельно рекомендуется поднимать отдельную клику.

{% endnote %}

{% if audience == "internal" %}
Для запуска запроса нужно:

1. Перейти на страницу YQL в кластере.

2. Отправить запрос, который начинается с `USE chyt.<cluster_name>[/<alias>];`, где:
  - `<cluster_name>` — имя кластера {{product-name}};
  - `<alias>` — алиас клики.

  Например: `USE chyt.yt_cluster/my_clique_alias`

{% note info "Примечание" %}

В случае публичной клики `/ch_public` название можно опустить. Таким образом, запись `USE chyt.yt_cluster;` эквивалента `USE chyt.yt_cluster/ch_public;`.

{% endnote %}

{% endif %}

## HTTP-интерфейс { #http }

Доступ к клике осуществляется через тяжёлые HTTP-прокси {{product-name}}. Самый удобный способ обратиться к клике — отправить запрос на имя соответствующего кластера и обработать перенаправление (redirect). В случае утилиты cURL это означает, что необходимо указать флаг `--location-trusted`.

Ниже смотрите пример, как задать с помощью утилиты cURL командной строки запрос к клике, зная имя кластера (`YT_PROXY`), токен доступа к {{product-name}} (`YT_TOKEN`) и алиас клики (`CHYT_ALIAS`).

```bash
$ curl --location-trusted -H "Authorization: OAuth $YT_TOKEN" "$YT_PROXY/chyt?chyt.clique_alias=$CHYT_ALIAS" -d 'SELECT Avg(a) FROM "//sys/clickhouse/sample_table"'
224.30769230769232
```

### Как указать алиас клики { #clique_alias_specification }

Алиас клики можно указать в специальном url-параметре `chyt.clique_alias` (см. пример выше), либо в поле `user` любым из способов авторизации (информация о фактическом пользователе содержится внутри токена). Например `http://cluster.domain.com/chyt?user=my-clique&password=my-token`.

{% note warning "Внимание" %}

Не допускается аутентификация (алиас клики + токен) через разные связки параметров. Например, следующий запрос содержит ошибку: `-H 'X-ClickHouse-User: my-clique' 'http://cluster.domain.com/chyt?password=my-token'` (алиас указан в заголовках `X-ClickHouse-User/Key`, а токен url-параметрах `user/password`)

{% endnote %}

{% note info "Примечание" %}

В старой версии запросы идут по пути `http://$YT_PROXY/query`.

При обращении к CHYT по устаревшему пути `/query` алиас передаётся через url-параметр `database`. При обращении по актуальному пути `/chyt` алиас берётся из `user` или из url-параметра `chyt.clique_alias`.

{% endnote %}

### Эксклюзивный порт для подключения { #chyt_port }

Не все клиенты поддерживают кастомные пути. Поэтому, на HTTP Proxy могут быть сконфигурированы специальные порты для обращения к CHYT, которые не требуют указания кастомных путей. Конкретные номера выделенных портов стоит уточнить у администратора {{product-name}} кластера. Про то, как настроить такие порты, будет написано в ближайшее время.

## ODBC-интерфейс { #odbc }

CHYT можно пользоваться из Tableau посредством [Clickhouse ODBC Driver](https://github.com/ClickHouse/clickhouse-odbc).

Для этого достаточно составить ODBC-конфигурацию, представленную ниже. Переменная `YT_TOKEN` обозначает токен доступа к {{product-name}} (`AQAD-qJ…`), `CHYT_ALIAS` — алиас клики (`ch_public`), `YT_PROXY` — имя кластера.

```bash
[ClickHouse]
Driver = <path to driver libclickhouseodbc>
# For example /usr/local/opt/clickhouse-odbc/lib/libclickhouseodbc.dylib
user = $CHYT_ALIAS
password = $YT_TOKEN
url = http://$YT_PROXY/chyt
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

JDBC-драйвер конфигурируется схожим образом с ODBC-драйвером, смотрите пример ниже. Переменная `YT_TOKEN` обозначает токен доступа к {{product-name}} (`AQAD-qJ…`),  `CHYT_ALIAS` — идентификатор либо алиас клики (`ch_public`), `YT_PROXY` — имя кластера.


```bash
JDBC URL = jdbc:clickhouse://{host}:{port}/chyt?chyt.clique_alias={clique_alias}
host = $YT_PROXY
user = $CHYT_ALIAS
password = $YT_TOKEN
port = 80
```

Кроме того, необходимо изменить свойства драйвера:

- `check_for_redirects=true`;
- `use_path_as_db=false`.

JDBC-драйвер можно использовать как основу при использовании open-source программы для работы с базами данных [DBeaver](https://dbeaver.io). Тестирование проводилось на версии DBeaver 6.1.5. В случае если ClickHouse-JDBC драйвер не устанавливается автоматически, его необходимо [скачать](https://github.com/yandex/clickhouse-jdbc) вручную. При этом необходимо использовать версию драйвера старше 0.1.55.

## CLI и Python API { #cli-and-api }

У CHYT есть официальный API для языка Python и официальная утилита командной строки. Подробнее можно прочитать [CLI и Python API](../../../../user-guide/data-processing/chyt/cli-and-api.md).
