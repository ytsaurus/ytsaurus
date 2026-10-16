# Environment variables in {{product-name}} Flow (Java)

The Java companion supports a set of environment variables to control the application and JVM behavior. You need to set the environment variables for the **worker** process.

## General variables {#general}

#|
|| **Variable** | **Description** | **Default** ||
|| `YT_FLOW_COMPANION_JOB_TTL` | TTL for the Job cache, format {number}{unit}. | `10m` ||
|#

## Variables for diagnostics and JVM {#diagnostics}

For more details about default JVM options, profiling, and configuration, see the section [Java companion profiling](../../../flow/java/profiling.md).

#|
|| **Variable** | **Description** | **Default** ||
|| `YT_FLOW_COMPANION_LOG_DIR` | Directory for companion logs and JVM diagnostic files. | `./logs` ||
|| `YT_FLOW_COMPANION_JFR_DISABLED` | If set to `1`, disables Java Flight Recorder options. | JFR is enabled ||
|| `YT_FLOW_COMPANION_JFR_OPTS` | Custom JFR options separated by a space, replacing the default values. Ignored if `YT_FLOW_COMPANION_JFR_DISABLED` is set. | [see profiling](../../../flow/java/profiling.md#jfr-defaults) ||
|| `YT_FLOW_COMPANION_GC_LOG_DISABLED` | If set to `1`, disables GC logging. | GC logging is enabled ||
|| `YT_FLOW_COMPANION_GC_LOG_OPTS` | Custom GC logging options separated by a space, replacing the default values. Ignored if `YT_FLOW_COMPANION_GC_LOG_DISABLED` is set. | [see profiling](../../../flow/java/profiling.md#gc-defaults) ||
|| `YT_FLOW_COMPANION_JVM_EXTRA_OPTS` | Additional JVM options added after all default options. | — ||
|#