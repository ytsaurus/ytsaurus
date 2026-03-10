# Isolating queries within a YQL-agent

{% note warning %}

The following functionality is available for queries in Query Tracker version [0.1.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fquery-tracker%2F0.1.2) and later.

{% endnote %}

By default, a YQL-agent runs all queries as threads within a single process. This may lead to memory leaks or cause all queries to fail because an error in one query.

Query isolation prevents these issues by executing each query in a separate process. To enable this feature, you need to [override](#override) the configuration of the YQL-agent.

For more information about the general mechanism for overriding cluster component configurations, see the [relevant section](../admin-guide/config-overrides.md).

## How to override a YQL-agent configuration { #override }

1. Create a `*.yaml` file with a [Kubernetes ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) as shown in the [example](../admin-guide/config-overrides.md#example).
1. Add a `ytserver-yql-agent.yson` key if the ConfigMap doesn't already include it. Otherwise, edit the existing key.
1. In the `process_plugin_config` field, add a configuration:

    {% cut "Sample YSON configuration" %}

    ```json
    ytserver-yql-agent.yson: |
        {"yql_agent"={
            "process_plugin_config" = {  
                "enabled" = %true;
                "log_manager_template" = {
                    "abort_on_alert" = %true;
                    "compression_thread_count" = 4;
                    "rules" = [
                        {
                            "exclude_categories" = [
                                "Bus";
                                "Concurrency";
                            ];
                            "family" = "plain_text";
                            "min_level" = "debug";
                            "writers" = [
                                "debug-yql-plugin";
                            ];
                        };
                        {
                            "family" = "plain_text";
                            "min_level" = "error";
                            "writers" = [
                                "stderr-yql-plugin";
                            ];
                        };
                        {
                            "family" = "plain_text";
                            "min_level" = "info";
                            "writers" = [
                                "info-yql-plugin";
                            ];
                        };
                    ];
                    "writers" = {
                        "debug-yql-plugin" = {
                            "file_name" = "/yt/yql-agent-slots-root-path/yql-agent.debug.log";
                            "type" = "file";
                        };
                        "info-yql-plugin" = {
                            "file_name" = "/yt/yql-agent-slots-root-path/yql-agent.log";
                            "type" = "file";
                        };
                        "stderr-yql-plugin" = {
                            "file_name" = "/yt/yql-agent-slots-root-path/yql-agent.error.log";
                            "type" = "file";
                        };
                    };
                };
                "slot_count" = 8;
                "slots_root_path" = "/yt/yql-agent-slots-root-path";
            };

            // other parameters of YQL-agent

        };}

    ```

    {% endcut %}

1. Reference the ConfigMap in the YTsaurus specification as shown in [Configuration overrides](../admin-guide/config-overrides.md#example).
