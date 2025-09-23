# Чтение с удаленных кластеров через RPC Proxy

Начиная с версии {{product-name}} 25.2 поддержана возможность через RPC Proxy делать запросы на удаленные (remote) кластеры.

## Настройка

Данная функциональность настраивается через [динамический конфиг](../../admin-guide/cluster-operations.md#ytdyncfgen).

Для RPC-проксей конфигурация данной функциональности находится по пути `/api/multiproxy` в динамическом конфиге RPC-проксей. В случае RPC Proxy, встроенной в Job Proxy, конфигурация находится по пути `/exec_node/job_controller/job_proxy/job_proxy_api_service/multiproxy` в динамическом конфиге нод.

Конфигурация состоит из двух частей:
1. Описание пресетов с разрешенными методами.
2. Маппинг из имени кластера в имя пресета.

Пример конфигурации для чтения с кластера `remote_cluster` с использованием SDK:
```
{
  "presets" = {
    "allow_sdk_read" = {
      "enabled_methods" = "read";
      "method_overrides" = {
        "StartTransaction" = "explicitly_enabled";
        "AbortTransaction" = "explicitly_enabled";
        "AttachTransaction" = "explicitly_enabled";
        "PingTransaction" = "explicitly_enabled";
        "LockNode" = "explicitly_enabled";
      }
    }
  };
  "cluster_presets" = {
    "remote_cluster" = "allow_sdk_read";
  }
}
```

Кластер `remote_cluster` должен присутствовать в списке кластеров в `//sys/clusters`.
