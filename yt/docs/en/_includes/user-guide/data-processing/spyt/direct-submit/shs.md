# Spark History Server (SHS)

To use the History Server in this mode, it must be launched separately.

The main parameters of the `shs-launch-yt` command are a subset of the parameters of the [spark-launch-yt](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md#spark-launch-yt-params) command that are relevant for launching the History Server. Example of launching the History Server:

```bash
$ shs-launch-yt --proxy <proxy address> --discovery-path <discovery path>
```

Example:

```bash
$ shs-launch-yt --proxy my.ytsaurus.cluster.net --discovery-path //home/user/spark/discovery
```

To save event logs for a task launch, the `spark-submit` command should include the following parameters:
`--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=ytEventLog:/<history server discovery path>/logs/event_log_table`

In this case, the event logs for the launch will be saved in the corresponding table and will be available in the History Server, which uses this table as a log storage.

{% include [shs warning](../common/shs-disclaimer.md) %}
