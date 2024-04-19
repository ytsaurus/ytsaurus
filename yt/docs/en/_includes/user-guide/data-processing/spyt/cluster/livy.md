# Livy server

Starting with version 1.74.0, SPYT comes with [Livy](https://livy.apache.org/), a service that allows communication between the client and a Spark cluster over a REST interface. The [Query tracker](../../../../../user-guide/query-tracker.md) module uses this functionality to execute Spark SQL queries in {{product-name}}.

The Livy distribution is already included in the release image of SPYT and can be found on the {{product-name}} cluster at the path `//home/spark/livy/livy.tgz`.

## Running Livy {#start}

To run the Livy server, specify the `--enable-livy` option in the command for starting a SPYT cluster. The maximum number of concurrent connections to the server is controlled by the `--livy-max-sessions` option. Attempting to establish a connection after reaching the limit will result in an error.

The driver for Spark jobs executed through Livy is deployed in the same container as the server. For this reason, to ensure correct calculation and allocation of resources in {{product-name}}, use the `--livy-driver-cores` and `--livy-driver-memory` options at cluster startup to configure the number of cores and driver memory size.

```bash
$ spark-launch-yt ... --enable-livy --livy-max-sessions 5 --livy-driver-cores 1 --livy-driver-memory 1G
```

To retrieve the address of the Livy server or other components, run the `spark-discovery-yt` command.

## Using Livy in Query Tracker {#query-tracker}

You can send queries to a running SPYT cluster from Query Tracker (SPYT tab) using Spark SQL. To do this, set the `cluster` (if your {{product-name}} installation doesn't have a default cluster) and `discovery_path` fields in `settings`. In addition, you can use the `spark_conf` field to pass an arbitrary Spark session configuration as a YSON map.

QT version 0.0.5 added support for authenticating and reusing sessions:

1. When Query Tracker starts executing a query, it issues the user a temporary [token](../../../../../user-guide/storage/auth.md) with a TTL of tens of minutes. The received token is used to authenticate the user in {{product-name}} when executing the query on a SPYT cluster. This ensures that the user has the necessary [permissions](../../../../../user-guide/storage/access-control.md#authorization) when reading or writing data. If query execution takes a long time, the temporary token's TTL will be periodically extended.

2. If `session_reuse` is set to true (default value) in `settings`, queries don't close the established cluster connection and reuse it in the future if possible. This reduces query execution time by 10–20 seconds. However, keep in mind that idle sessions also count toward the limit on the number of concurrent connections to the cluster (`livy-max-sessions`). The session is automatically terminated if no new queries are received for more than 10 minutes.

## Configuring a session when connecting to the server directly {#direct-connect}

Livy server endpoints are described in the [official documentation](https://livy.apache.org/docs/latest/rest-api.html).

To use {{product-name}}, during the initialization of a Livy session, specify two configuration parameters — the paths to the Java (`spark.yt.jars`) and Python (`spark.yt.pyFiles`) libraries — in the `spark_conf` field:

```python
data = {'kind': 'spark', 'conf': {'spark.yt.version': '1.75.4', 'spark.yt.jars': 'yt:///home/spark/spyt/releases/1.75.4/spark-yt-data-source.jar', 'spark.yt.pyFiles': 'yt:///home/spark/spyt/releases/1.75.4/spyt.zip'}}
req = requests.post(host + '/sessions', data=json.dumps(data))
resp = req.json()
```

## Sparkmagic {#sparkmagic}

You can connect to the Livy server via [Sparkmagic](https://github.com/jupyter-incubator/sparkmagic) to work with a SPYT cluster in a Jupyter notebook over a REST interface. This reduces the number of network accesses required to use interactive Python while maintaining all functionality. In addition to Python, Sparkmagic also supports Scala and SQL.
