# How to try

There are several ways to try CHYT.

The simplest way is to run a query via the YQL web interface. You can take a public clique with the `*ch_public` alias as a clique. This is the main public clique found on every cluster of CHYT's presence.

{% note warning "Attention" %}

All {{product-name}} users have access to the public clique, so there is a risk that it may not be available due to queries from other users or be occupied by other people's computations. In this regard, important processes and dashboards must not be based on a public clique. A public clique is essentially a demo version of CHYT. We highly recommend creating a separate click for your team.

{% endnote %}

To run a query, you need to:

1. Go to the YQL page in the cluster.

2. Send a query starting with `USE chyt.<cluster_name>[/<alias>];` where:
- `<cluster_name>`: The {{product-name}} cluster name.
- `<alias>`: The alias clique without a `*` symbol if you want to specify a particular clique.

For example: `USE chyt.yt_cluster/my_clique_alias`

{% note info "Note" %}

In case of the public `/ch_public` clique, the name can be omitted. Thereby, the `USE chyt.yt_cluster;` entry is equivalent to `USE chyt.yt_cluster/ch_public;`.

{% endnote %}

## HTTP interface { #http }

A clique is accessed via heavy {{product-name}} HTTP proxies. The most convenient way to access a clique is to send a query to the name of the corresponding cluster and process the redirect. In case of the curl utility, this means that you must specify the `--location-trusted` flag.

Below is an example of how to query a clique using the curl command line utility, knowing the cluster name (`YT_PROXY`), the {{product-name}} access token (`YT_TOKEN`), and the clique alias (`CHYT_ALIAS`).

```bash
curl --location-trusted -H "Authorization: OAuth $YT_TOKEN" "$YT_PROXY/query?database=$CHYT_ALIAS" -d 'SELECT Avg(a) FROM "//sys/clickhouse/sample_table"'
224.30769230769232
```

## ODBC interface { #odbc }

You can use CHYT from Tableau via the [Clickhouse ODBC Driver](https://github.com/ClickHouse/clickhouse-odbc).

To do this, you just need to make the ODBC configuration represented below. The `YT_TOKEN` variable denotes the {{product-name}} access token (`AQAD-qJ…`), `CHYT_ALIAS` — the (`*ch_public`) clique alias, and `YT_PROXY` — the cluster name.

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

Below is an example of how to access CHYT via the ODBC interface from the command line using the [isql](https://en.wikipedia.org/wiki/UnixODBC) utility. To do this, write the `~/.odbc.ini` configuration file, install the Clickhouse ODBC driver from the official repository, and use the isql utility.

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

## JDBC interface { #jdbc }

The JDBC driver is configured in a similar way to the ODBC driver, see the example below. The `YT_TOKEN` variable denotes the {{product-name}} access token (`AQAD-qJ…`), `CHYT_ALIAS` — the (`*ch_public`) clique ID or alias, `YT_PROXY` — the cluster name.


```bash
JDBC URL = jdbc:clickhouse://{host}:{port}/query?database={database}
host = $YT_PROXY
user = default
password = $YT_TOKEN
database = $CHYT_ALIAS
port = 80
```

Besides that, you need to change the driver properties:

- `check_for_redirects=true`;
- `use_path_as_db=false`.

The JDBC driver can be used as a basis for using[DBeaver](https://dbeaver.io), an open-source database management program. DBeaver 6.1.5 was used for testing. If the ClickHouse-JDBC driver is not installed automatically, [download](https://github.com/yandex/clickhouse-jdbc) it manually. You must use a driver version higher than 0.1.55.

## CLI and Python API { #cli-and-api }

CHYT has the official Python API and the official command line utility. For more information, see [CLI and Python API](../../../../user-guide/data-processing/chyt/cli-and-api.md).







