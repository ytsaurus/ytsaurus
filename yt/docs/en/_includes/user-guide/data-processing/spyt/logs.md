# Logs

## Event log  { #event-log }
*Event log* is a special log format in Spark. Logs are written by a [driver](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-app) and read by a special service called *Spark History Server (SHS)*. SHS launches alongside the master and the workers when a SPYT cluster is started. SHS uses the log to draw a UI similar to Spark UI.

The difference between them is that Spark UI is available on the driver only while a job is running, whereas SHS is always up and can read the logs of completed jobs.
Logs are written to a [dynamic table](../../../../user-guide/dynamic-tables/overview.md) in `discovery-path`. SHS can delete them [automatically](#log-settings).

## Enabling { #log-on }

Set `spark.eventLog.enabled` to `true` on starting a job:
```bash
$ spark-submit-yt ... --conf spark.eventLog.enabled=true
```


## Configuring { #log-settings }

For a detailed description of SHS settings, please see the [Spark documentation](https://spark.apache.org/docs/latest/monitoring.html#spark-history-server-configuration-options). You can pass any settings from this description in to `spark-launch-yt ... --params '{spark_conf={"spark.param"=value;...}}'`.

By default, the `spark.history.fs.cleaner.enabled` is set to `true`. The other parameters' default values are as per the Spark documentation. If the parameters are set to their defaults, SHS deletes all logs older than one week. You can change this age limit using the setting: `spark.history.fs.cleaner.maxAge` (`spark-launch-yt ... --params '{spark_conf={"spark.history.fs.cleaner.maxAge"="14d";...}}'`).


## Worker logs {# worker-logs }

When you start a cluster, you can configure [worker](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone) logs to upload to {{product-name}} tables. Logs will be written to `{cluster_path}/logs/worker_log` with a separate table created for each date. To enable sending of logs when starting a cluster, you must use one of these options:
* `--enable-worker-log-transfer`: Enables log forwarding.
* `--enable-worker-log-json-mode`: Enables json mode for the logs. Table data are displayed in a more convenient format and broken down into component parts. If the option is disabled, logs are written to a single column called `messages`.
* `--worker-log-update-interval ...` : Sets the frequency of log upload; the default value is 10 minutes (10m), the minimum value is 1 minute.
* `--worker-log-table-ttl ...` : Sets the table age limit; the default is 7 days (7d).

You can view logs on SHS: the *executors* tab of any *application* contains `[HS]`-prefixed links for every worker. A log page referenced by these links is generated from the {{product-name}} tables; therefore, it is available even if a worker no longer is. A log persists until the relevant table is deleted:

```bash
spark-launch-yt ... \
  --enable-worker-log-transfer \
  --enable-worker-log-json-mode \
  --worker-log-update-interval 30m \
  --worker-log-table-ttl 30d
```

