# How to try

There are several ways to try CHYT.

The simplest way is to run a query via the {% if audience == "internal" %}YQL{% else %}Query Tracker{% endif %} web interface. You can use a public clique with the `ch_public` alias. This is the main public clique found on every cluster of CHYT's presence.

{% note info "Note" %}

In {{product-name}} 24.1 and lower, a clique alias is specified with an asterisk at the beginning. For example, `*ch_public`. In the current version, `*ch_public` and `ch_public` are the same alias.

{% endnote %}

{% note warning "Attention" %}

All {{product-name}} users have access to the public clique, so there is a risk that it may not be available due to queries from other users or be occupied by other people's computations. In this regard, important processes and dashboards must not be based on a public clique. A public clique is essentially a demo version of CHYT. We highly recommend creating a separate clique for your team.

{% endnote %}

{% if audience == "internal" %}
To run a query, you need to:

1. Go to the YQL page in the cluster.

2. Send a query starting with `USE chyt.<cluster_name>[/<alias>];` where:
  - `<cluster_name>`: The {{product-name}} cluster name.
  - `<alias>`: The clique alias.

  For example: `USE chyt.yt_cluster/my_clique_alias`

{% note info "Note" %}

In case of the public `/ch_public` clique, the name can be omitted. Thereby, the `USE chyt.yt_cluster;` entry is equivalent to `USE chyt.yt_cluster/ch_public;`.

{% endnote %}

{% endif %}

## HTTP interface { #http }

A clique is accessed via heavy {{product-name}} HTTP proxies. The most convenient way to access a clique is to send a query to the name of the corresponding cluster and process the redirect. When using the cURL utility, this means that you must specify the `--location-trusted` flag.

Below is an example of how to query a clique using the cURL command line utility, knowing the cluster name (`YT_PROXY`), the {{product-name}} access token (`YT_TOKEN`), and the clique alias (`CHYT_ALIAS`).

```bash
$ curl --location-trusted -H "Authorization: OAuth $YT_TOKEN" "$YT_PROXY/chyt?chyt.clique_alias=$CHYT_ALIAS" -d 'SELECT Avg(a) FROM "//sys/clickhouse/sample_table"'
224.30769230769232
```

### How to specify a clique alias { #clique_alias_specification }

You can specify a clique alias in the special `chyt.clique_alias` URL parameter (see the example above) or in the `user` field using any of the authorization methods (information about the actual user is contained in the token). For example, `http://cluster.domain.com/chyt?user=my-clique&password=my-token`.

{% note warning "Attention" %}

Authentication (clique alias + token) via different combinations of parameters isn't allowed. For example, the following query contains an error: `-H 'X-ClickHouse-User: my-clique' 'http://cluster.domain.com/chyt?password=my-token'` (the alias is specified in the `X-ClickHouse-User/Key` headers, and the token is specified in the `user/password` URL parameters)

{% endnote %}

{% note info "Note" %}

In the old version, queries use the `http://$YT_PROXY/query` path.

When CHYT is accessed using the deprecated `/query` path, the alias is passed via the `database` url parameter. When CHYT is accessed using the new `/chyt` path, the alias is taken from `user` or the `chyt.clique_alias` URL parameter.

{% endnote %}

### Exclusive connection port { #chyt_port }

Not all clients support custom paths. Special ports can be configured on the HTTP Proxy to access CHYT. These ports don't require custom paths to be specified. For the numbers of dedicated ports, please contact the {{product-name}} cluster administrator. The instructions on how you can configure these ports will be added in the near future.

## ODBC interface { #odbc }

You can use CHYT from Tableau via the [Clickhouse ODBC Driver](https://github.com/ClickHouse/clickhouse-odbc).

To do this, you just need to make the ODBC configuration represented below. The `YT_TOKEN` variable denotes the {{product-name}} access token (`AQAD-qJ…`), `CHYT_ALIAS` is the alias of the clique (`ch_public`), and `YT_PROXY` is the cluster name.

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

The JDBC driver is configured in a similar way to the ODBC driver, see the example below. The `YT_TOKEN` variable denotes the {{product-name}} access token (`AQAD-qJ…`), `CHYT_ALIAS` is the ID/alias of the clique (`ch_public`), and `YT_PROXY` is the cluster name.


```bash
JDBC URL = jdbc:clickhouse://{host}:{port}/chyt?chyt.clique_alias={clique_alias}
host = $YT_PROXY
user = $CHYT_ALIAS
password = $YT_TOKEN
port = 80
```

Besides that, you need to change the driver properties:

- `check_for_redirects=true`;
- `use_path_as_db=false`.

The JDBC driver can be used as a basis for using [DBeaver](https://dbeaver.io), an open-source database management program. DBeaver 6.1.5 was used for testing. If the ClickHouse-JDBC driver is not installed automatically, [download](https://github.com/yandex/clickhouse-jdbc) it manually. You must use a driver version higher than 0.1.55.

## CLI and Python API { #cli-and-api }

CHYT has the official Python API and the official command line utility. For more information, see [CLI and Python API](../../../../user-guide/data-processing/chyt/cli-and-api.md).
