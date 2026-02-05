# Установка микросервисов

## Описание

Для {{product-name}} есть набор дополнительных микросервисов, которые расширяют функциональность веб-интерфейса и предоставляют полезную информацию для администраторов и пользователей кластера.

На данный момент поддерживаются следующие компоненты:

- Resource Usage: Позволяет отслеживать и анализировать потребление дискового пространства по аккаунтам. [Код микросервиса](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/resource_usage/json_api_go), [код препроцессинга](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/resource_usage_roren).
- Access Log Viewer: Позволяет смотреть access-логи мастера в UI. [Код микросервиса](https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/access_log_viewer/http_service), [код препроцессинга](https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/access_log_viewer/preprocessing), [код парсера сырых логов](https://github.com/ytsaurus/ytsaurus/tree/main/yt/microservices/access_log_viewer/raw_access_log_preprocessing).
- Bulk ACL Checker: Предоставляет оптимизированный способ проверки прав доступа (ACL) для пользователя по множеству путей. Снижает нагрузку на мастер. [Код микросервиса](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/bulk_acl_checker/http_service_go), [код препроцессинга](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/bulk_acl_checker_roren).
- Id To Path Updater: Вспомогательный фоновый процесс, который парсит access-логи мастера и создает в динамической таблице сопоставление `Node ID` → `Path`. Является зависимостью для других микросервисов. [Код препроцессинга](https://github.com/ytsaurus/ytsaurus/blob/main/yt/microservices/id_to_path_mapping/id_to_path_updater).

## Архитектура

Микросервисы {{product-name}} обычно состоят из двух частей:

1.  Preprocessing (`CronJob`): Фоновый процесс, запускаемый по расписанию. Он собирает и обрабатывает данные (например, из логов или снапшотов мастера) и сохраняет их в подготовленном виде в таблицы.
2.  API (`Deployment`): Постоянно работающий веб-сервис, который предоставляет удобный доступ к данным, подготовленным на этапе Preprocessing.

## Предварительные требования

Перед установкой убедитесь, что выполнены следующие условия:

1. Установлен Helm 3.x.
2. Версия {{product-name}} Kubernetes оператора не меньше 0.28.0.
3. Включена [загрузка снапшотов](../../admin-guide/persistence-uploader.md#uploading-snapshots-to-cypress) и [access-логов](../../admin-guide/logging.md#structured_log_delivery) мастера в Кипарис.

  Настройка обоих процессов требует перезагрузки мастера с даунтаймом, поэтому с целью его минимизации рекомендуется производить применение настроек одновременно.

  Для включения этих функций добавьте соответствующие настройки сайдкаров и логирования в spec.primaryMasters:

  ```yaml
  spec:
    primaryMasters:
      hydraPersistenceUploader:
        image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
      timbertruck:
        image: ghcr.io/ytsaurus/sidecars:{{sidecars-version}}
      structuredLoggers:
        - name: access
          minLogLevel: info
          category: Access
          format: json
          rotationPolicy:
            maxTotalSizeToKeep: 5_000_000_000
            rotationPeriodMilliseconds: 900000
      locations:
        - locationType: Logs
          path: /yt/master-logs
      volumeMounts:
        - name: master-logs
          mountPath: /yt/master-logs
      volumeClaimTemplates:
        - metadata:
            name: master-logs
          spec:
            accessModes: [ "ReadWriteOnce" ]
            resources:
              requests:
                storage: 5Gi
  ```

4. Установлен [Cron Helm-чарт](../../admin-guide/install-cron.md#process_master_snapshot), в котором включена задача `process_master_snapshot`. Эта задача должна отработать хотя бы один раз и создать необходимые директории и таблицы, а именно: `//sys/admin/snapshots/snapshot_exports` и `//sys/admin/snapshots/user_exports`.
5. Установлен [CHYT](../../admin-guide/install-chyt.md) и [создана Клика](../../user-guide/data-processing/chyt/try-chyt.md). Сохраните название клики в переменную окружения `CHYT_CLIQUE_NAME`. Для простоты в примере будет использоваться клика с названием `ch_public`, создаваемая по умолчанию.

```bash
export CHYT_CLIQUE_NAME=ch_public
```

Проверим статус задачи `process_master_snapshot`:

```bash
kubectl -n <namespace> get cronjobs ytsaurus-cron-cron-chart-process-master-snapshot -o jsonpath='{.status}'
```

Ожидаем ненулевой `lastSuccessfulTime`:

```json
{
  "lastScheduleTime": "2025-12-01T10:00:00Z",
  "lastSuccessfulTime": "2025-12-01T10:05:09Z"
}
```

Проверим наличие необходимых директорий и таблиц:

```bash
yt list //sys/admin/snapshots/snapshot_exports
yt list //sys/admin/snapshots/user_exports
```

Должны увидеть хотя бы один снапшот с именем вида `000000068.snapshot_3163fafb_unified_export` и `000000068.snapshot_3163fafb_user_export` соответственно.

## Подготовка и установка

### Шаг 1: Подготовка пользователей, выдача прав (ACL) и настройка окружения

Для каждого микросервиса требуется свой пользователь-робот:

- `robot-msvc-access-log-viewer` - для Access Log Viewer;
- `robot-msvc-acl-checker` - для Bulk ACL Checker;
- `robot-msvc-id-to-path` - для Id To Path Updater;
- `robot-msvc-resource-usage` - для Resource Usage.

Создайте пользователей [согласно инструкции](../../user-guide/storage/auth.md):

```bash
yt create user --attr "{name=robot-msvc-access-log-viewer}"
yt create user --attr "{name=robot-msvc-acl-checker}"
yt create user --attr "{name=robot-msvc-id-to-path}"
yt create user --attr "{name=robot-msvc-resource-usage}"
```

Создайте рабочие директории для микросервисов в `//sys/admin/yt-microservices`:

```bash
yt create map_node //sys/admin/yt-microservices/access_log_viewer --recursive
yt create map_node //sys/admin/yt-microservices/bulk_acl_checker
yt create map_node //sys/admin/yt-microservices/node_id_dict
yt create map_node //sys/admin/yt-microservices/raw_master_access_log_processing
yt create map_node //sys/admin/yt-microservices/resource_usage
```

Создайте директории для распаршенных access-логов мастера:

```bash
yt create map_node //sys/admin/logs/export/master-access-parsed/1d --recursive
yt create map_node //sys/admin/logs/export/master-access-parsed/30min
```

Создайте рабочие директории временных файлов для микросервисов:

```bash
yt create map_node //sys/admin/yt-microservices/tmp/access_log_viewer/30min --recursive
yt create map_node //sys/admin/yt-microservices/tmp/access_log_viewer/1d
yt create map_node //sys/admin/yt-microservices/tmp/id_to_path_updater
```

{% note info %}

Для рабочих директорий микросервисов рекомендуется завести отдельный аккаунт и выдать ACL `use` на него для соответствующих роботов. В этом примере для простоты мы будем работать с аккаунтом `sys`.

{% endnote %}

Выдайте права доступа для каждого пользователя.

- Для Resource Usage:

Выдайте права на чтение снапшотов, доступ к директориям микросервиса и использование аккаунта.

```bash
yt set //sys/admin/snapshots/snapshot_exports/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read]}'
yt set //sys/admin/yt-microservices/node_id_dict/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read]}'
yt set //sys/admin/yt-microservices/resource_usage/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[read; write; create; remove; mount]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-resource-usage]; permissions=[use]}'
```

- Для Bulk ACL Checker:

Выдайте права на чтение снапшотов, доступ к директориям микросервиса и использование аккаунта.

```bash
yt set //sys/admin/snapshots/user_exports/@acl/end '{action=allow; subjects=[robot-msvc-acl-checker]; permissions=[read]}'
yt set //sys/admin/yt-microservices/bulk_acl_checker/@acl/end '{action=allow; subjects=[robot-msvc-acl-checker]; permissions=[read; write; create; remove; mount]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-acl-checker]; permissions=[use]}'
```

- Для Id To Path Updater:

Включите `bulk_insert` для пользователя и для всего кластера (если он еще не включен).

```bash
yt set //sys/users/robot-msvc-id-to-path/@enable_bulk_insert %true
yt set //sys/@config/tablet_manager/enable_bulk_insert %true
```

Выдайте права на чтение и маркировку логов, запись в рабочую директорию микросервиса и использование аккаунта и бандла.

```bash
yt set //sys/admin/logs/export/master-access/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[read; write]}'
yt set //sys/admin/yt-microservices/node_id_dict/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[read; write; create; remove; mount]}'
yt set //sys/admin/yt-microservices/tmp/id_to_path_updater/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[read; write; create; remove]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[use]}'
yt set //sys/tablet_cell_bundles/sys/@acl/end '{action=allow; subjects=[robot-msvc-id-to-path]; permissions=[use]}'
```

- Для Access Log Viewer:

  Выдайте права на чтение и маркировку логов, запись в рабочую директорию микросервиса, использование аккаунта и доступ к клике.

  ```bash
  yt set //sys/admin/logs/export/master-access/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write]}'
  yt set //sys/admin/yt-microservices/tmp/access_log_viewer/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; create; remove]}'
  yt set //sys/admin/yt-microservices/access_log_viewer/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; create; remove; mount]}'
  yt set //sys/admin/yt-microservices/raw_master_access_log_processing/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; create; remove]}'
  yt set //sys/admin/logs/export/master-access-parsed/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[read; write; remove]}'
  yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[use]}'
  yt set //sys/access_control_object_namespaces/chyt/$CHYT_CLIQUE_NAME/principal/@acl/end '{action=allow; subjects=[robot-msvc-access-log-viewer]; permissions=[use]}'
  ```

  Укажите для CHYT клики [словарь](../../user-guide/data-processing/chyt/reference/configuration.md#clickhouse_config), который сможет обращаться к Bulk ACL Checker'у:

  1. Добавьте словарь `ACL`:

    ```
    if [ $(yt exists //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/dictionaries) == "false" ]; then
        yt set --format json //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/dictionaries '[]' --recursive
    fi

    yt set --format json //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/dictionaries/end '{
        "layout": {
            "complex_key_cache": {
                "size_in_cells": 1000000
            }
        },
        "name": "ACL",
        "lifetime": 300,
        "structure": {
            "attribute": [
                {
                    "name": "action",
                    "type": "String",
                    "null_value": "NoAnswer"
                }
            ],
            "key": {
                "attribute": [
                    {
                        "name": "cluster",
                        "type": "String"
                    },
                    {
                        "name": "subject",
                        "type": "String"
                    },
                    {
                        "name": "path",
                        "type": "String"
                    }
                ]
            }
        },
        "source": {
            "http": {
                "url": "http://ytsaurus-msvc-ytmsvc-chart-bulk-acl-checker-service.default.svc.cluster.local:80/clickhouse-dict",
                "format": "JSONEachRow"
            }
        }
    }'
    ```

  2. Включите `cancel_http_readonly_queries_on_client_close`:

    ```bash
    yt set -r //sys/strawberry/chyt/$CHYT_CLIQUE_NAME/speclet/clickhouse_config/settings/cancel_http_readonly_queries_on_client_close 1
    ```

  В `dictionaries[].source.http.url` указан адрес до Bulk ACL Checker, который будет развернут в namespace `default`. Если используется иной - укажите его.

### Шаг 2: Подготовка Kubernetes Secret с токенами

По умолчанию используется один общий секрет с названием `ytsaurus-msvc`. Каждый микросервис и его препроцессинг использует токен из своей переменной окружения.

- Для Resource Usage — `YT_RESOURCE_USAGE_TOKEN`;
- Для Bulk ACL Checker — `YT_BULK_ACL_CHECKER_TOKEN`;
- Для ID To Path Updater — `YT_ID_TO_PATH_TOKEN`;
- Для Access Log Viewer — `YT_ACCESS_LOG_VIEWER_TOKEN`.

Пример создания секрета. Команда сама выпустит необходимые токены и подставит их в секрет:

```bash
kubectl create secret generic ytsaurus-msvc \
  --from-literal=YT_RESOURCE_USAGE_TOKEN="$(yt issue-token robot-msvc-resource-usage)" \
  --from-literal=YT_BULK_ACL_CHECKER_TOKEN="$(yt issue-token robot-msvc-acl-checker)" \
  --from-literal=YT_ID_TO_PATH_TOKEN="$(yt issue-token robot-msvc-id-to-path)" \
  --from-literal=YT_ACCESS_LOG_VIEWER_TOKEN="$(yt issue-token robot-msvc-access-log-viewer)" \
  -n <namespace>
```

При необходимости можно создать отдельные секреты, указав их в `.microservices.<name>.secretRefs`.

### Шаг 3: Подготовка `values.yaml`

- Укажите прокси и имя кластера:

```yaml
cluster:
  proxy: "http-proxies.default.svc.cluster.local" # Внутренний адрес HTTP-прокси для namespace `default`
  name: "<cluster-name>"                          # Имя вашего кластера (см. в `//sys/@cluster_connection/cluster_name`)
```

- Настройте CORS:

  Необходимо указать `cors.allowedHostSuffixes` или `cors.allowedHosts` вашего UI. Подробнее будет описано ниже.

- Настройте Access Log Viewer:

  Вместо `ch_public` укажите вашу клику из `CHYT_CLIQUE_NAME`.

  ```yaml
  microservices:
    accessLogViewer:
      api:
        config:
          chytAlias: "ch_public"
  ```

Остальные параметры работают "из коробки". Подробнее в разделе с [детальной конфигурацией](#detailed-config).

- Пример минимального `values.yaml`:

  ```yaml
  cluster:
    proxy: "http-proxies.default.svc.cluster.local"
    name: "<cluster-name>"

  cors:
    allowedHostSuffixes:
      - "<your-ui-domain-suffix>"
  ```

Полный список параметров доступен в [values.yaml](https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docker/charts/ytmsvc-chart/values.yaml) в репозитории чарта. Детальное описание представлено ниже в разделе [Детальная конфигурация](#detailed-config).

### Шаг 4: Установка Helm-чарта

```bash
helm install ytsaurus-msvc oci://ghcr.io/ytsaurus/ytmsvc-chart \
  --version {{ytmsvc-version}} \
  -f values.yaml \
  -n <namespace>
```

Посмотреть список созданных чартом pods и cronjobs:
```bash
kubectl get pod -l app.kubernetes.io/name=ytmsvc-chart -n <namespace>
kubectl get cronjobs -l app.kubernetes.io/name=ytmsvc-chart -n <namespace>
```

Вручную запустить cron-задачу можно встроенными средствами `kubectl`:
```bash
kubectl create job --from=cronjob/<cron-job-name> <your-job-name> -n <namespace>
```

Посмотреть логи процессинга и микросервиса:
```bash
kubectl logs ytsaurus-msvc-resource-usage-preprocessing-29390072-nrhj6 -n <namespace>
kubectl logs deployment/ytsaurus-msvc-ytmsvc-chart-resource-usage-api -n <namespace>
```

### Шаг 5: Настройка сетевого доступа и включение в UI

Resource Usage API и Access Log Viewer API используется веб-интерфейсом {{product-name}} напрямую из браузера пользователя. Выберите способ доступа в зависимости от вашего окружения.

#### Production

Рекомендуемый способ для production. Микросервисы будут доступны на том же домене, что и UI, по пути `/resource-usage/` и `/access-log-viewer/` соответственно.

1. Настройте `apiPrefix` в `values.yaml`:

  ```yaml
  microservices:
    resourceUsage:
      api:
        config:
          apiPrefix: "/resource-usage/"
  ```

  Для Access Log Viewer'а префикс настраивать не надо.

2. Примените изменения:

```bash
helm upgrade ytsaurus-msvc oci://ghcr.io/ytsaurus/ytmsvc-chart \
  --version {{ytmsvc-version}} \
  -f values.yaml \
  -n <namespace>
```

3. Модифицируйте Ingress:

Необходимо модифицировать нынешний манифест UI Ingress так, чтобы он перенаправлял запросы `/resource-usage` на сервис:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: ui-ingress
  namespace: <namespace>
spec:
  rules:
    - host: <your-ui-domain> # Укажите здесь ваш домен UI
      http:
        paths:
          - backend:
              service:
                name: ytsaurus-msvc-ytmsvc-chart-resource-usage-service
                port:
                  name: http
            path: /resource-usage
            pathType: Prefix
          - backend:
              service:
                name: ytsaurus-msvc-ytmsvc-chart-access-log-viewer-service
                port:
                  name: http
            path: /access-log-viewer
            pathType: Prefix
          - backend:
              service:
                name: ytsaurus-ui
                port:
                  name: http
            path: /
            pathType: Prefix
```

Примените манифест:

```bash
kubectl apply -f ui-ingress.yaml
```

4. Укажите URL в конфигурации UI:

```bash
yt set //sys/@ui_config/resource_usage_base_url '"https://<your-ui-domain>/resource-usage/"'
yt set //sys/@ui_config/access_log_viewer_base_url '"https://<your-ui-domain>/access-log-viewer/"'
```

#### Testing

Этот способ подходит для быстрой проверки работоспособности.

1. Настройте CORS в `values.yaml`:

Так как UI (например, `http://localhost:8080`) будет обращаться к `http://localhost:3000`, браузер заблокирует запрос без явного разрешения CORS. Разрешите обращения от UI:

```yaml
cors:
  allowedHosts:
    - "localhost:8080"
microservices:
  resourceUsage:
    api:
      config:
        apiPrefix: "/"
```

2. Примените изменения:

```bash
helm upgrade ytsaurus-msvc oci://ghcr.io/ytsaurus/ytmsvc-chart \
  --version {{ytmsvc-version}} \
  -f values.yaml \
  -n <namespace>
```

3. Запустите проброс портов:

```bash
kubectl port-forward service/ytsaurus-msvc-ytmsvc-chart-resource-usage-service 3000:80 -n <namespace>
kubectl port-forward service/ytsaurus-msvc-ytmsvc-chart-access-log-viewer-service 3001:80 -n <namespace>
```

4. Укажите локальный адрес в конфигурации UI:

```bash
yt set //sys/@ui_config/resource_usage_base_url '"http://localhost:3000/"'
yt set //sys/@ui_config/access_log_viewer_base_url '"http://localhost:3001/"'
```

#### Проверка результата

- Работа Resource Usage:

  1. Откройте веб-интерфейс {{product-name}};
  2. Перейдите в раздел `Accounts` и выберите любой аккаунт;
  3. Сверху откройте вкладку `Detailed usage`.

- Работа Access Log Viewer:

  1. Откройте веб-интерфейс {{product-name}};
  2. Перейдите в раздел `Navigation` и откройте интересующий вас объект;
  3. Сверху откройте вкладку `Access log`.

{% note info %}

Для появления вкладки `Detailed usage` и `Access log` может потребоваться время (не более нескольких минут) и обновление страницы.

Если вкладка не появилась или что-то пошло не так, проверьте:
- Правильность `resource_usage_base_url` (должен включать протокол и завершаться `/`)
- Логи микросервиса: `kubectl logs deployment/ytsaurus-msvc-ytmsvc-chart-resource-usage-api -n <namespace>`
- Работу Ingress, если он используется: `kubectl get ingress -n <namespace>`

{% endnote %}

## Детальная конфигурация {#detailed-config}

### Общие параметры

  ```yaml
  cluster:
    # Внутренний адрес HTTP-прокси
    proxy: "http-proxies-lb.default.svc.cluster.local"
    # Имя кластера
    name: "ytsaurus"

  cors:
    # Списки хостов/суффиксов для CORS-запросов (необходимы для интеграции с UI)
    allowedHosts: []
    allowedHostSuffixes: []

  # Кука, используемая для аутентификации пользователей
  authCookieName: "YTCypressCookie"

  # Секреты, которые будут доступны из подов
  secretRefs: [ytsaurus-msvc]
  ```

### Resource Usage API

  Конфигурация API-сервиса Resource Usage:

  ```yaml
  microservices:
    resourceUsage:
      api:
        config:
          # Адрес и порт для основного HTTP-сервера API
          httpAddr: "[::]:80"

          # Таймаут для обработчиков HTTP-запросов
          httpHandlerTimeout: 120s

          # Адрес и порт для сбора метрик
          debugHttpAddr: "[::]:81"

          # Префикс обработчиков HTTP-сервера
          apiPrefix: "/"

          # Имя cookie для авторизации (должно совпадать с настройкой UI)
          authCookieName: YTCypressCookie

          # Путь для хранения обработанных снапшотов
          snapshotRoot: //sys/admin/yt-microservices/resource_usage

          # Список полей для исключения из ответов API
          excludedFields: []
  ```

### Resource Usage: очистка старых снапшотов

  Конфигурация автоматической очистки старых снапшотов.

  ```yaml
  microservices:
    resourceUsage:
      removeExcessive:
        # Включить очистку старых снапшотов
        enabled: true

        config:
          # Коэффициент частоты прореживания: чем больше, тем меньше старых снапшотов останется
          denominator: 1.4

          # Коэффициент увеличения временного интервала при углублении в прошлое
          stepSizeIncrease: 2.86

          # Период, в течение которого свежие снапшоты не удаляются
          ignoreWindowSize:
            days: 3

          # Размер первого временного окна для прореживания
          firstStepWindowSize:
            weeks: 1

          # Минимальный интервал между снапшотами в первом окне
          firstStepAllowedFrequency:
            hours: 6
  ```

### Id To Path Updater

  Конфигурация микросервиса Id To Path Updater:

  ```yaml
  microservices:
    idToPathUpdater:
      config:
        # Путь к access-логам мастера
        inputTablesSource: //sys/admin/logs/export/master-access

        # Динамическая таблица для соответствия cluster → node_id → path (создается автоматически)
        outputTable: //sys/admin/yt-microservices/node_id_dict/data

        # Директория для временных файлов
        tmpPath: //sys/admin/yt-microservices/tmp/id_to_path_updater
  ```

### Access Log Viewer API

  Конфигурация API-сервиса Access Log Viewer:

  ```yaml
  microservices:
    accessLogViewer:
      api:
        enabled: true
        config:
          # Имя используемой клики
          chytAlias: "ch_public"
  ```

### Raw Log Preprocessing

  Конфигурация парсера сырых логов доступа:

  ```yaml
  microservices:
    accessLogViewer:
      rawLogPreprocessing:
        config:
          # Директория с временными файлами работы препроцессинга
          workPath: "//sys/admin/yt-microservices/raw_master_access_log_processing"
          # Количество одновременно обрабатываемых таблиц с сырыми логами
          maxInputTables: 16
  ```

### Access Log Viewer Preprocessing

  Конфигурация препроцессинга Access Log Viewer:

  ```yaml
  microservices:
    accessLogViewer:
      preprocessing:
        config:
          # Количество одновременно запускаемых операций
          maxParallelOps: 1
          # Количество одновременно обрабатываемых таблиц
          maxTables: 100
  ```

Все параметры можно посмотреть в [исходном коде](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/charts/ytmsvc-chart/values.yaml).
