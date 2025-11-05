# Spark History Server (SHS)

History Server can be part of an internal cluster or run separately. The second case is necessary if you run tasks [directly in {{product-name}}](../../../../../user-guide/data-processing/spyt/launch.md#submit).

## Saving event logs when running tasks directly in {{product-name}} {#shs-direct-submit}

To save the event logs of a task run, the `spark-submit` command should include the following parameters:
`--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=ytEventLog:/<history server discovery path>/logs/event_log_table`

In this case, the task run event logs will be saved in the corresponding table and will be available in the History Server, which uses this table as a log storage.

{% note info "Note" %}

When you start the History Server, it indexes all entries from the event logs source, and processing each entry may take 10–15 seconds. New logs become available only after the complete indexing of all existing data is finished.

In clusters with high load, this can lead to significant delays when starting or restarting the History Server. Therefore, for production clusters with intensive task flows, it is recommended to use a new (empty) event logs table when starting a new Spark cluster. If necessary, the old table can be archived (move the table).

{% endnote %}

## Running a separate History Server {#shs-launch-yt}

The main parameters of the `shs-launch-yt` command are a subset of the parameters of the [spark-launch-yt](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md#spark-launch-yt-params) command that are relevant for running the History Server. Example of running a History Server:

```bash
$ shs-launch-yt --proxy <proxy address> --discovery-path <discovery path>
```

Example:

```bash
$ shs-launch-yt --proxy my.ytsaurus.cluster.net --discovery-path //home/user/spark/discovery
```