# Установка Cron

## Описание

Для {{product-name}} есть набор cron-задач, которые полезны при эксплуатации кластера, таких как очистка временных директорий или удаление неактивных нод. Поддерживаются как встроенные, так и пользовательские задачи.

Список встроенных скриптов:

* `clear_tmp` - скрипт, который очищает временные файлы на кластере. Код скрипта живет [здесь](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cron/clear_tmp).
* `prune_offline_cluster_nodes` - скрипт, который удаляет из Кипариса offline ноды. Код скрипта живет [здесь](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cron/prune_offline_cluster_nodes).

## Предварительные требования

На данном этапе у вас должны быть:

* Helm 3.x
* запущенный кластер {{product-name}} и адрес HTTP прокси (`http_proxy`);
* специальный пользователь-робот для Cron с выписанным для него токеном (см. раздел [Управление токенами](../../user-guide/storage/auth.md#token-management)).

## Базовая установка

```bash
helm install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  --set yt.proxy="http_proxy" \
  --set yt.token="<ROBOT-CRON-TOKEN>"
```

## Конфигурация

Все параметры чарта можно задать через `values.yaml` или с помощью `--set` в командной строке.

## Аутентификация в YTsaurus

Укажите токен напрямую:

```yaml
yt:
  proxy: yt.company.com
  token: Qwerty123!
```

Или настройте использование Kubernetes-секрета:

```yaml
unmanagedSecret:
  enabled: true
  secretKeyRef:
    name: ytadminsec
    key: token
```

## Встроенные задачи { #jobs }

Каждая задача задаётся структурой:
- `enabled`: Включена ли задача
- `args`: Аргументы командной строки
- `schedule`: Расписание в формате cron
- `restartPolicy`: Политика перезапуска (рекомендуется `Never`)

Пример включения задачи:

```bash
helm upgrade --install ytsaurus-cron oci://ghcr.io/ytsaurus/cron-chart \
  --version {{cron-version}} \
  --set jobs.clear_tmp_location.enabled=true \
  --set jobs.clear_tmp_location.schedule="*/30 * * * *"
```

## Пользовательские задачи { #additionalJobs }

Можно определить собственные задачи:

```yaml
additionalJobs:
  my_cleanup:
    enabled: true
    args:
      - clear_tmp
      - --directory "//my/custom/path"
    schedule: "0 */6 * * *"
    restartPolicy: Never
```

## Пример `values.yaml`

```yaml
yt:
  proxy: yt.mycompany.com
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
  -f my-values.yaml
```

## Часто используемые параметры

| Параметр                      | Описание                                         |
|------------------------------|--------------------------------------------------|
| `yt.proxy`                   | HTTP-прокси для доступа к {{product-name}}       |
| `yt.token`                   | Токен доступа (если `unmanagedSecret` отключён)  |
| `unmanagedSecret`            | Использовать Kubernetes-секрет                   |
| `image.repository`           | Образ Docker                                     |
| `image.tag`                  | Тег образа                                       |
| `schedule`                   | Расписание по умолчанию (если не указано в job)  |
| `concurrencyPolicy`          | `Allow`, `Forbid`, или `Replace`                 |
| `successfulJobsHistoryLimit`| Сколько успешных задач хранить                    |
| `failedJobsHistoryLimit`    | Сколько неудачных задач хранить                   |

