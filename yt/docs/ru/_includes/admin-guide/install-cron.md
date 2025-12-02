# Установка Cron

## Описание

Для {{product-name}} есть набор cron-задач, которые полезны при эксплуатации кластера, таких как очистка временных директорий или удаление неактивных нод. Поддерживаются как встроенные, так и пользовательские задачи.

Список встроенных скриптов:

* `clear_tmp` - скрипт, который очищает временные файлы на кластере. Код скрипта живет [здесь](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cron/clear_tmp);
* `prune_offline_cluster_nodes` - скрипт, который удаляет из Кипариса offline ноды. Код скрипта живет [здесь](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cron/prune_offline_cluster_nodes).
* `validate_master_snapshot` и `export_master_snapshot` - скрипты для валидации и экспорта мастер-снапшотов в статическую таблицу. Эти скрипты используются в связке. Подробнее [здесь](#process_master_snapshot).

## Предварительные требования

На данном этапе у вас должны быть:

* Helm 3.x;
* запущенный кластер {{product-name}} и внутренний адрес HTTP прокси;
* специальный пользователь-робот `robot-cron` с выписанным для него токеном (см. раздел [Управление токенами](../../user-guide/storage/auth.md#token-management)):

  ```
  yt create user --attr "{name=robot-cron}"
  yt issue-token robot-cron
  ```

## Базовая установка

```bash
helm install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  --set yt.proxy="http://http-proxies.default.svc.cluster.local" \
  --set yt.token="<ROBOT-CRON-TOKEN>" \
  -n <namespace>
```

После этого можно проверить наличие соответствующих cronjobs:

```bash
kubectl get cronjobs -l app.kubernetes.io/name=cron-chart -n <namespace>
```

Вручную запустить cron-задачу можно встроенными средствами `kubectl`:

```bash
kubectl create job --from=cronjob/<cron-job-name> <your-job-name> -n <namespace>
```

## Конфигурация

Все параметры чарта можно задать через `-f values.yaml` или с помощью `--set` в командной строке.

## Аутентификация в YTsaurus

Укажите токен напрямую:

```yaml
yt:
  proxy: "http://http-proxies.default.svc.cluster.local"
  token: "<ROBOT-CRON-TOKEN>"
```

Или используйте Kubernetes-секрет:

1. Создайте секрет:

  ```bash
  kubectl create secret generic yt-robot-cron-token --from-literal=token="$(yt issue-token robot-cron)"
  ```

2. В `values.yaml` включите использование неуправляемых чартом секретов и укажите откуда брать токен:

  ```yaml
  unmanagedSecret:
    enabled: true
    secretKeyRef:
      name: yt-robot-cron-token
      key: token
  ```

## Определение задач { #jobs }

Каждая cron-задача определяется в `values.yaml` под уникальным именем. Встроенные задачи определяются в `.jobs`. Дополнительные пользовательские задачи рекомендуется определять в `.additionalJobs`.

### Простые задачи { #simple-jobs }

Запускает один скрипт с заданными аргументами.

Каждая задача задаётся структурой:
- `enabled`: Включена ли задача.
- `args`: Аргументы командной строки.
- `schedule`: Расписание в формате cron.
- `restartPolicy`: Политика перезапуска (рекомендуется `Never`).

Для диагностики можно проверить состояние подов:

```
kubectl get pods -n <namespace> -l job-name=<job-name>
```

Чтобы посмотреть логи конкретного пода, достаточно указать его имя. Так как под содержит только один контейнер, передавать название контейнера не требуется:

```
kubectl logs <pod-name> -n <namespace>
```

Например:

```
kubectl logs ytsaurus-cron-cron-chart-clear-tmp-trash-29409690-vqsh5 -n <namespace>
```

### Последовательные задачи { #sequential-jobs }

Позволяет выполнить несколько скриптов строго по очереди в рамках одного запуска. Это полезно для связанных операций, например, сначала проверить снапшот, а потом экспортировать его в статическую таблицу.

Каждая задача задаётся структурой:
- `enabled`, `schedule`, `restartPolicy`: Аналогичны простой задаче.
- `jobs`: Список имен подзадач, которые будут выполнены в заданной последовательности.
- `jobDescriptions`: Словарь, где ключ — это имя подзадачи из списка `jobs`, а значение — её конфигурация (конфигурировать возможно только аргументы командной строки через `jobDescriptions.jobName.args`).

Так как все задачи выполняются в одном поде, для просмотра логов соответствующей джобы необходимо указать имя контейнера, которое соответствует именам из `jobs.X.jobs`. Для задачи `process_master_snapshot`, спецификация которой описана ниже, именами контейнеров будут являться `validate` и `export`.

```yaml
jobs:
  process_master_snapshot:
    enabled: false
    jobs:
      - validate
      - export
    jobDescriptions:
      validate:
        args:
          - validate_master_snapshot
      export:
        args:
          - export_master_snapshot
    schedule: "0 * * * *"
    restartPolicy: Never
```

Посмотреть логи валидации, соответственно:

```
kubectl logs ytsaurus-cron-cron-chart-process-master-snapshot-29409720-9dmps -c validate -n <namespace>
```

Название контейнера будет соответствовать названию джобы из `jobs`.

## Встроенные задачи { #enabling-builtin-jobs }

Пример включения задачи в `values.yaml`:

```yaml
jobs:
  clear_tmp_location:
    enabled: true
    schedule: "*/30 * * * *"
```

### Процессинг мастерных снапшотов { #process_master_snapshot }

Соответствует ключу `process_master_snapshot` в `values.yaml`.

Для своей работы требует:
- Включенной [загрузки мастерных снапшотов в Кипарис](../../admin-guide/persistence-uploader.md#uploading-snapshots-to-cypress).
- `read`, `write` и `remove` прав на `//sys/admin/snapshots`:

  ```bash
  yt set //sys/admin/snapshots/@acl/end '{action=allow; subjects=[robot-cron]; permissions=[read; write; remove;]}'
  ```

- `use` на аккаунт, которому принадлежит `//sys/admin/snapshots` (`sys` по умолчанию):

  ```bash
  yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-cron]; permissions=[use]}'
  ```

В ноду `//sys/admin/snapshots/<cell_id>/snapshots` поставляются сырые мастерные снапшоты. Этот процесс проводит их валидацию и экспорт в человеко-читаемый формат: снапшот Кипариса сохраняется в `//sys/admin/snapshots/snapshot_exports`, а снапшот пользователей и относящихся к ним ACL групп в `//sys/admin/snapshots/user_exports`.

По умолчанию процессинг выключен.

Чтобы переопределить аргументы командной строки, можно передать соответствующие аргументы в `values.yaml`:

```yaml
jobs:
  process_master_snapshot:
    enabled: true
    jobDescriptions:
      export:
        args:
          - "export_master_snapshot"
          - "--memory-limit-gbs"
          - "4"
```

## Пользовательские задачи { #additionalJobs }

Можно определить собственные задачи:

```yaml
additionalJobs:
  my_cleanup:
    enabled: true
    args:
      - clear_tmp
      - --directory
      - //my/custom/path
    schedule: "0 */6 * * *"
    restartPolicy: Never

  my_new_process:
    enabled: true
    jobs:
      - process_one
      - process_two
    jobDescriptions:
      process_two:
        args:
          - process_two
      process_one:
        args:
          - process_one
    schedule: "0 * * * *"
    restartPolicy: Never
```

## Пример `values.yaml`

```yaml
yt:
  proxy: http://http-proxies.default.svc.cluster.local
  token: my-secret-token

jobs:
  clear_tmp_files:
    enabled: true
    args:
      - clear_tmp
      - --directory "//tmp/yt_wrapper/file_storage"
      - --account "tmp_files"
    schedule: "*/30 * * * *"
    restartPolicy: Never

unmanagedSecret:
  enabled: false
```

Запуск:

```bash
helm upgrade --install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  -f values.yaml
```

## Часто используемые параметры

| Параметр                     | Описание                                           |
| ---------------------------- | -------------------------------------------------- |
| `yt.proxy`                   | HTTP-прокси для доступа к {{product-name}}         |
| `yt.token`                   | Токен доступа (если `unmanagedSecret` отключён)    |
| `unmanagedSecret`            | Использовать Kubernetes-секрет                     |
| `image.repository`           | Образ Docker                                       |
| `image.tag`                  | Тег образа                                         |
| `schedule`                   | Расписание по умолчанию (если не указано в job)    |
| `concurrencyPolicy`          | `Allow`, `Forbid`, или `Replace`                   |
| `successfulJobsHistoryLimit` | Сколько успешных задач хранить                     |
| `failedJobsHistoryLimit`     | Сколько неудачных задач хранить                    |
| `ttlSecondsAfterFinished`    | Как долго объекты job будут храниться в Kubernetes |
