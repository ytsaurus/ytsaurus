# Java companion profiling in {{product-name}} Flow (Java)

Java companion includes continuous profiling based on [JDK Flight Recorder](https://wiki.openjdk.org/spaces/jmc/pages/37584926/Overview) (JFR). Use profiling to diagnose performance issues and analyze incidents.

## Capabilities {#capabilities}

JFR files contain comprehensive telemetry for the Java application and JVM. This lets you:

- Analyze performance — identify CPU/RAM-intensive methods and bottlenecks.
- Diagnose memory and GC issues.
- Investigate incidents after the fact using data from the failure period.

JFR has low overhead and is a standard tool for monitoring Java applications in production.

You can open and analyze the resulting JFR files with [Java Mission Control (JMC)](https://wiki.openjdk.org/spaces/jmc/overview), the standard tool for analyzing JFR recordings.

## How it works {#how-it-works} {#jfr-defaults}

Profiling is enabled by default via the following JVM options:

```
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
-XX:StartFlightRecording=disk=true,settings=profile,maxage=24h,maxsize=1000m,dumponexit=true,filename=<logDir>/dump.jfr
-XX:FlightRecorderOptions=repository=<logDir>/jfr,maxchunksize=30M
```

JFR is written to disk in 30 MB chunks to the directory specified by the `YT_FLOW_COMPANION_LOG_DIR` environment variable. Data is kept for up to 24 hours and is limited to a total size of 1 GB. You manage JFR via [environment variables](../../../flow/java/env-variables.md):

- `YT_FLOW_COMPANION_JFR_DISABLED` — if set to `1`, it completely disables JFR options.
- `YT_FLOW_COMPANION_JFR_OPTS` — custom JFR options separated by spaces, which replace the default values (ignored if `YT_FLOW_COMPANION_JFR_DISABLED` is set).

## Other default diagnostic JVM options {#default-jvm-options}

### GC logging {#gc-defaults}

Enabled by default:

```
-Xlog:gc:file=<logDir>/gc.log:time,uptime:filecount=10,filesize=50m
```

Where `<logDir>` is the value of the `YT_FLOW_COMPANION_LOG_DIR` environment variable.

You can override this via `YT_FLOW_COMPANION_GC_LOG_OPTS` or disable it via `YT_FLOW_COMPANION_GC_LOG_DISABLED=1`.

### Crash dump and OOM handling {#crash-defaults}

Always enabled:

```
-XX:+ExitOnOutOfMemoryError
-XX:+CreateCoredumpOnCrash
-XX:ErrorFile=<logDir>/hs_err_%p.log
```

Where `<logDir>` is the value of the `YT_FLOW_COMPANION_LOG_DIR` environment variable.

### Heap dump configuration (recommended) {#heap-dump}

Heap dump options are **not** set by default and must be configured via the `YT_FLOW_COMPANION_JVM_EXTRA_OPTS` environment variable:

```
YT_FLOW_COMPANION_JVM_EXTRA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=<heapDumpDir>"
```

Where `<heapDumpDir>` is the path to the directory for storing the heap dump.

{% note warning %}

A heap dump can be very large — up to the full size of the heap. We recommend allocating a separate persistent volume for the heap dump.

{% endnote %}

## Getting profiling data {#get-profiling-data}

### Via SSH {#via-ssh}

Connect to the worker via SSH and copy the JFR files to your development machine from the directory specified by the `YT_FLOW_COMPANION_LOG_DIR` environment variable.

### Via CLI {#via-cli}

To download the latest complete JFR chunk from the companion, run this command:

```bash
# <pipeline_path> - path to the pipeline directory in YT.
# <worker> - IP address of the worker with rpc_port, for example "[IPv6]:port".

ya run yt/yt/flow/tools/download_jfr --pipeline-path <pipeline_path> --worker <worker>
```

You can copy the ready-to-use command for downloading JFR from each companion in the YT web interface, on the pipeline page, in the **Workers** tab.