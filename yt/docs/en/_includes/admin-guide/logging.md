# Configuring server component logging

The server components of the {{product-name}} cluster generate detailed logs that can be used to audit and analyze problems during operation. For production installations, we recommend allocating dedicated [storage locations on persistent volumes](../../admin-guide/locations.md) for these logs. Absence of logs can significantly complicate support.

You can use Prometheus metrics with the `yt_logging_*` prefix to analyze the logging subsystem.

## Debugging logs { #debug_logs }

Debugging logs are described in the `loggers` section of the {{product-name}} component specification.

<small>Table 1 — `Ytsaurus` debug logger settings</small>

| **Field** | **Possible values** | **Description** |
| ------------------- | --------------- | ------- |
| `name` | arbitrary string | The logger name (we recommend choosing short and clear names like `debug` or `info`). |
| `format` | `plain_text` (default), `yson`, `json` | The format of the log string. |
| `minLogLevel` | `trace`, `debug`, `info`, `error` | The minimum level for records that reach the log. |
| `categoriesFilter` |   | A filter that only lets you write logs from some subsystems (see [below](#debug_log_categories)). |
| `writerType` | `file`, `stderr` | Write logs to a file or stderr. When writing to stderr, the rotation settings are ignored. |
| `compression` | `none` (default), `gzip`, `zstd` | If a value other than `none` is set, the {{product-name}} server will write compressed logs. |
| `useTimestampSuffix` | `true`, `false` (default) | If `true`, a timestamp is added to the file name when it's opened or on rotation. At the same time, the numbering mechanism doesn't apply to old segments. This option is only relevant when writing to a file. |
| `rotationPolicy` |   | Log rotation settings (see [below](#log_rotation)). This option is only relevant when writing to a file. |


The path to the directory for logs with `writerType=file` is set in the `Logs` type location description. If no `Logs` location is specified, they are written to `/var/log`.

Log file names follow the format `[component].[name].log(.[format])(.[compression])(.[timestamp_suffix])`. Examples:
- `controller-agent.error.log`
- `master.debug.log.gzip`
- `scheduler.info.log.json.zstd.2023-01-01T10:30:00`

Debug log entries contain the following fields:
- `instant` — the time in the local time zone
- `level` — write level: `T` — trace, `D` — debug, `I` — info, `W` — warning, `E` — error
- `category` — the name of the subsystem the recording belongs to (for example, `ChunkClient`, `ObjectServer`, or `RpcServer`)
- `message` — message body
- `thread_id` — the ID or name of the thread that generated the entry (only written as `plain_text`)
- `fiber_id` — the ID of the fiber that generated the record (only written as `plain_text`)
- `trace_id` — the trace_context ID the recording appeared for (only written as `plain_text`)

{% cut "Sample entry" %}
```
2023-09-15 00:00:17,215385      I       ExecNode        Artifacts prepared (JobId: d15d7d5f-164ff08a-3fe0384-128e0, OperationId: cd56ab80-d21ef5ab-3fe03e8-d05edd49, JobType: Map)      Job     fff6d4149ccdf656    2bd5c3c9-600a44f5-de721d58-fb905017
```
{% endcut %}

### Recommendations for configuring categories { #debug_log_categories }

There are two types of category filters (`categoriesFilter`):
- inclusive — records are only written for categories that were explicitly listed
- exclusive — records are written for any categories except those that are listed

In large installations, you often need to exclude the `Bus` and `Concurrency` categories.

{% cut "Sample filters" %}
```yaml
categoriesFilter:
  type: exclude
  values: ["Bus", "Concurrency"]

categoriesFilter:
  type: include
  values: ["Scheduler", "Strategy"]
```
{% endcut %}


## Structured logs { #structured_logs }
Some {{product-name}} components can generate structured logs, which you can later use for auditing, analytics, and automatic processing. Structured logs are described in the `structured_loggers` section of the {{product-name}} component specification.

Structured loggers are described using the same fields as debugging logs, except:
- `writerType` — not set (structured logs are always written to a file)
- `categoriesFilter` — the required `category` field is set instead and is equal to one category

Structured logs should always be written in a structured format: `JSON` or `YSON`. Events in a structured log are usually recorded at the `info` level. The set of structured log fields varies depending on the specific log type.

The main types of structured logs:
- `master_access_log` — data access log (written on the master, `Access` category)
- `master_security_log` — log of security events like adding a user to a group or modifying an ACL (written on the master, `SecurityServer` category)
- `structured_http_proxy_log` — log of requests to http proxy, one line per request (written on http proxy, `HttpStructuredProxy` category)
- `chyt_log` — log of requests to CHYT, one line per request (written on http proxy, `ClickHouseProxyStructured` catgeory)
- `structured_rpc_proxy_log` — log of requests to rpc proxy, one line per request (written on rpc proxy, `RpcProxyStructuredMain` category)
- `scheduler_event_log` — scheduler event log, written by the scheduler (`SchedulerEventLog` category)
- `controller_event_log` — log of controller agent events, written on the controller agent (`ControllerEventLog` category)

## Configuring log rotation { #log_rotation }
For debug and structured logs written to a file, you can configure the built-in rotation mechanism (the `rotationPolicy` field). The rotation settings are detailed in the table. If the `useTimestampSuffix` option isn't enabled, an index number is appended to the file names of old segments on rotation.

<small>Table 2 — Log rotation settings </small>

| **Field** | **Description** |
| ------------------- |  ------- |
| `rotationPeriodMilliseconds` | Rotation period in milliseconds. Can be set together with `maxSegmentSize`. |
| `maxSegmentSize` | Log segment size limit in bytes. Can be set together with `rotationPeriodMilliseconds`. |
| `maxTotalSizeToKeep` | Total segment size limit in bytes. At the time of rotation, the oldest logs are deleted to meet the limit. |
| `maxSegmentCountToKeep` | Limit on the number of stored log segments. The oldest segments over the limit are deleted. |


## Dynamic configuration { #log_dynamic_configuration }
Components that support dynamic configuration let you further refine the logging system settings using the dynamic config in Cypress (`logging` section).

Basic parameters:
- `enable_anchor_profiling` — enables Prometheus metrics for individual record prefixes
- `min_logged_message_rate_to_profile` — the minimum message frequency for inclusion in a separate metric
- `suppressed_messaged` — a list of debug log message prefixes to be excluded from logging

Configuration example:
```yson
{
  logging = {
    enable_anchor_profiling = %true;
    min_logged_message_rate_to_profile = 100;
    suppressed_messaged = [
      "Skipping out of turn block",
      "Request attempt started",
      "Request attempt acknowledged"
    ];
  }
}
```

## Sample logging settings { #logging_examples }
```yaml
  primaryMasters:
    ...
    loggers:
	  - name: debug
        compression: zstd
        minLogLevel: debug
        writerType: file
        rotationPolicy:
          maxTotalSizeToKeep: 50_000_000_000
          rotationPeriodMilliseconds: 900000
        categoriesFilter:
          type: exclude
          values: ["Bus", "Concurrency", "ReaderMemoryManager"]
      - name: info
        minLogLevel: info
        writerType: file
        rotationPolicy:
    	  maxTotalSizeToKeep: 10_000_000_000
          rotationPeriodMilliseconds: 900000
      - name: error
        minLogLevel: error
        writerType: stderr
    structuredLoggers:
      - name: access
        minLogLevel: info
        category: Access
        rotationPolicy:
          maxTotalSizeToKeep: 5_000_000_000
          rotationPeriodMilliseconds: 900000
    locations:
      - locationType: Logs
        path: /yt/logs
      - ...

    volumeMounts:
      - name: master-logs
        mountPath: /yt/logs
      - ...

    volumeClaimTemplates:
      - metadata:
          name: master-logs
        spec:
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: 100Gi
      - ...
```
