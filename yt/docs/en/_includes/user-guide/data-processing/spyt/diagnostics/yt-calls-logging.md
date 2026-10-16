# Logging {{product-name}} calls (available from SPYT 2.11.0)

SPYT can log every RPC call to {{product-name}}: the call type, request ID, and duration. For reading and writing tables, it also logs the ypath. This is the main way to understand how much time a job spends on cluster calls and which tables it accesses.

## Enabling { #enable }

Logging is enabled via the system property `spyt.yt.calls.log.level` — separately for the driver and executors:

```bash
--conf spark.driver.extraJavaOptions=-Dspyt.yt.calls.log.level=debug
--conf spark.executor.extraJavaOptions=-Dspyt.yt.calls.log.level=debug
```

If the property is not set, the level of these loggers matches the level of the root logger in the logging profile — that is, the default behavior remains unchanged.

The property is supported in all logging profiles that come with SPYT. This means it works both for the driver and executors in direct submission, and for components of a SPYT standalone cluster. Both regular table reads and writes, as well as distributed read and write modes, are logged.

{% note warning "Warning" %}

The `debug` level generates dozens of log lines for each table operation. Enable it for one‑time diagnostics of a specific job, not on a permanent basis.
{% endnote %}

## What is logged { #loggers }

| Logger | What it logs |
| --- | --- |
| `tech.ytsaurus.client.rpc.DefaultRpcBusClient` | Every RPC request and its duration |
| `tech.ytsaurus.client.rpc.FailoverRpcExecutor` | Retry attempts for requests |
| `tech.ytsaurus.spyt.wrapper` | ypath and request ID for read calls |
| `tech.ytsaurus.spyt.format.YtOutputWriter`, `tech.ytsaurus.spyt.format.YtDistributedOutputWriter` | ypath and request ID for writes |

## Example output { #example }

```
26/07/30 13:46:47 DEBUG YtWrapper$: Formatting path ytTable:/tmp/keepling/spyt-1152/2-debug-on/dst
26/07/30 13:46:47 DEBUG YtWrapper$: Formatting path /tmp/keepling/spyt-1152/2-debug-on/dst
26/07/30 13:46:47 DEBUG DefaultRpcBusClient: Sending request `ApiService/LockNode/b4-7e2cf11b-894d8647-fa6ae0dc` Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@a56d753)
26/07/30 13:46:47 DEBUG DefaultRpcBusClient: Request `ApiService/LockNode/b4-7e2cf11b-894d8647-fa6ae0dc` finished in 478 ms Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@a56d753)
26/07/30 13:46:47 DEBUG YtWrapper$: YT partition tables: #5a9acae4-1a5-139f0191-e941a650, splitBytes: 268435456, enableCookies: false, requestId: b9-82970b11-ccb8c93d-c261d589
26/07/30 13:46:47 DEBUG DefaultRpcBusClient: Sending request `ApiService/PartitionTables/b9-82970b11-ccb8c93d-c261d589` Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@4aabcb7b)
26/07/30 13:46:48 DEBUG DefaultRpcBusClient: Request `ApiService/PartitionTables/b9-82970b11-ccb8c93d-c261d589` finished in 474 ms Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@4aabcb7b)
```

The call duration is logged by the client — this is the `Request ... finished in N ms` line. It is linked to a specific table via the request ID: in the example, the `PartitionTables` call with the ID `b9-82970b11-ccb8c93d-c261d589` took 474 ms, and the `YT partition tables` line with the same ID shows which ypath it was made for.
