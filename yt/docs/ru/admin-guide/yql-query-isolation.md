# Изоляция запросов в YQL‑агенте

{% note warning %}

Описанная ниже функциональность доступна для запросов в Query Tracker начиная с версии [0.1.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fquery-tracker%2F0.1.2).

{% endnote %}

По умолчанию YQL‑агент запускает все запросы в потоках одного процесса. Это может приводить к утечкам памяти или сбою всех запросов из-за ошибки в одном из них.

Решить эти проблемы позволяет изоляция запросов — когда каждый запрос выполняется в отдельном процессе. Для включения этой возможности нужно [переопределить](#override) конфигурацию YQL‑агента.

Подробнее про общий механизм переопределения конфигурации компонентов кластера можно прочитать в [соответствующем разделе](../admin-guide/config-overrides.md).

## Как переопределить конфигурацию YQL‑агента { #override }

1. Создайте `*.yaml` файл с [Kubernetes ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/), как описано в [примере](../admin-guide/config-overrides.md#example).
1. Добавьте в ConfigMap ключ `ytserver-yql-agent.yson` (если его ещё нет) или отредактируйте уже существующий.
1. В поле `process_plugin_config` добавьте конфигурацию:

    {% cut "Пример YSON-конфигурации" %}

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

            // другие параметры YQL-агента

        };}

    ```

    {% endcut %}

1. Добавьте ссылку на ConfigMap в спецификацию Ytsaurus, как описано в разделе [Переопределение конфигурации](../admin-guide/config-overrides.md#example).
