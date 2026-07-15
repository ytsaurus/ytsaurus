# Административные команды CLI (`yt admin`)

Группа команд `yt admin` объединяет административные и диагностические утилиты для работы с кластером {{product-name}}. Команды доступны через стандартный CLI-клиент `yt` (пакет `ytsaurus-client`).

Некоторым командам нужны дополнительные зависимости (`kubernetes>=31.0.0` для `logs k8s`, `docker>=7.1.0` для `metrics replay`). Они устанавливаются вместе с [extra-набором `admin`](../../api/python/start.md#extras):

{% note warning "Внимание" %}

Большая часть команд `yt admin` (`describe`, `logs`, `metrics`) является **экспериментальной**, их интерфейс может измениться в будущих версиях. При запуске такой команды выводится предупреждение. Чтобы его отключить, установите переменную окружения:

```bash
export YT_SUPPRESS_ADMIN_EXPERIMENTAL_WARNING=1
```

{% endnote %}

## Общий интерфейс { #interface }

```bash
yt admin <admin_command> [<sub_command>] [options]
```

Список доступных подкоманд:

| Команда | Назначение |
| --- | --- |
| `switch-leader` | Сменить лидера мастер-селла |
| `describe` | Получить описание кластера или динамической таблицы (атрибуты, конфиги, состояние) |
| `logs` | Выгрузить логи компонентов кластера через Kubernetes API |
| `metrics` | Выгрузка и локальная визуализация метрик Prometheus |
| `remove-master-unrecognized-options` | Удалить нераспознанные опции из динамического конфига мастера |

Получить справку по любой команде можно через `--help`:

```bash
yt admin --help
yt admin describe --help
yt admin describe cluster --help
```

---

## Сменить лидера мастер-селла (`switch-leader`) { #switch-leader }

Переключает лидера указанного мастер-селла на заданный адрес.

```bash
yt admin switch-leader --cell-id <cell-id> --new-leader-address <address>
```

| Параметр | Описание |
| --- | --- |
| `--cell-id` | Идентификатор мастер-селла |
| `--new-leader-address` | Адрес мастера, который должен стать новым лидером |

### Пример

```bash
yt admin switch-leader \
  --cell-id 65726e65-ad6b7562-10259-79747361 \
  --new-leader-address ms-1.masters.ytsaurus-dev.svc.cluster.local:9010
```

---

## Получить описание кластера или динамической таблицы (`describe`) { #describe }

Собирает описание кластера или отдельных динамических таблиц (атрибуты, конфигурации, текущее состояние) и сохраняет его в файл.

Результат сохраняется в файл с временной меткой в имени, например `describe/cluster_2026-06-03T12-00-00Z.yson`.

Общие опции вывода для обеих подкоманд:

| Параметр | По умолчанию | Описание |
| --- | --- | --- |
| `--format {yson,json,yaml}` | `yson` | Формат выходного файла |
| `-o, --output <dir>` | `describe` | Каталог для сохранения результата |

### Описание кластера (`cluster`) { #describe-cluster }

Собирает общую информацию о компонентах кластера и их конфигурации.

```bash
yt admin describe cluster [--nodes] [--bundles] [--dynamic-configs] [--static-configs] [-o dir] [--format ...]
```

| Параметр | Описание |
| --- | --- |
| `--nodes` | Информация о нодах кластера (`//sys/cluster_nodes`) |
| `--bundles` | Информация о бандлах динамических таблиц (`//sys/tablet_cell_bundles`) |
| `--dynamic-configs` | Динамические конфигурации мастера, планировщика, контроллер-агентов, нод и прокси |
| `--static-configs` | Статические конфигурации нод (по одному на каждый flavor) |

Если не указан ни один из флагов `--nodes`, `--bundles`, `--dynamic-configs`, `--static-configs`, собирается **вся** информация сразу.

#### Примеры

Собрать информацию о кластере в YSON:

```bash
yt admin describe cluster
```

Собрать только информацию о нодах и бандлах в JSON в каталог `debug`:

```bash
yt admin describe cluster --nodes --bundles --format json -o debug
```

### Описание динамических таблиц (`table`) { #describe-table }

Собирает подробный набор атрибутов для одной или нескольких динамических таблиц, включая "тяжёлые" атрибуты (`tablets`, статистики по чанкам и сжатию). В качестве аргументов передаются один или несколько путей до динамических таблиц.

```bash
yt admin describe table <path> [<path> ...] [-o dir] [--format ...]
```

#### Пример

```bash
yt admin describe table //home/project/table1 //home/project/table2 --format yaml -o tables-dump
```

---

## Выгрузить логи компонентов кластера (`logs`) { #logs }

Выгружает логи компонентов кластера {{product-name}}, развёрнутого в Kubernetes. Доступен единственный backend — `k8s`, работающий через Kubernetes API.

```bash
yt admin logs k8s [options] <cluster_name> <component_name> [<group_name>]
```

Утилита извлекает имена подов и пути к директориям из спецификации кластера, после чего находит нужные логи в контейнере `ytserver`. Далее она делает одно из двух:
- скачивает файлы логов целиком;
- выполняет `grep` на стороне сервера и сохраняет только те строки, которые совпали с запросом.

### Требования

- Доступ к Kubernetes-кластеру (`kubeconfig` или in-cluster конфигурация).
- RBAC-права на `get` для CR `ytsaurus` и `exec` для подов в нужном namespace.
- Установленный python-пакет `kubernetes` (входит в `ytsaurus-client[admin]`).

### Позиционные аргументы

| Аргумент | По умолчанию | Описание |
| --- | --- | --- |
| `cluster_name` | — | Имя кластера {{product-name}} (имя CR, см. `kubectl get ytsaurus`) |
| `component_name` | — | Тип компонента (см. таблицу ниже) |
| `group_name` | `default` | Имя группы инстансов компонента |

### Опции

| Параметр | По умолчанию | Описание |
| --- | --- | --- |
| `-n, --namespace <ns>` | `default` | Пространство имен Kubernetes, в котором расположен кластер |
| `-p, --pods <pod>` | — | Конкретный под (можно указывать несколько раз) |
| `--exec-slot-index <N>` | — | Индекс слота для логов job-proxy (только для `exec_nodes`) |
| `--from-ts <ISO8601>` | — | Логи, изменённые после указанного времени, вида `2026-06-15T14:00:00Z` |
| `--to-ts <ISO8601>` | — | Логи, созданные до указанного времени, вида `2026-06-15T14:00:00Z` |
| `-w, --writer <name>` | — | Фильтр по имени writer'а из конфига (можно несколько раз). Ниже показан способ получения списка writer'ов |
| `--writer-force <regex>` | — | Принудительный regex для фильтрации имён файлов логов |
| `-o, --output <dir>` | `logs` | Каталог для сохранения логов |
| `--grep <regex>` | — | Серверный grep по содержимому (с авто-распаковкой `.zstd`/`.gz`) |
| `-y, --yes` | — | Пропустить начальный запрос подтверждения выгрузки. Не отключает запрос о перезаписи уже существующих локальных файлов |

### Поддерживаемые компоненты

| Ключ компонента | Короткое имя | Поле CR | Поле группы |
| --- | --- | --- | --- |
| `discovery` | `ds` | discovery | — |
| `primary_masters` | `ms` | primaryMasters | — |
| `master_caches` | `msc` | masterCaches | — |
| `http_proxies` | `hp` | httpProxies | `role` |
| `rpc_proxies` | `rp` | rpcProxies | `role` |
| `data_nodes` | `dnd` | dataNodes | `name` |
| `exec_nodes` | `end` | execNodes | `name` |
| `tablet_nodes` | `tnd` | tabletNodes | `name` |
| `queue_agents` | `qa` | queueAgents | — |
| `tcp_proxies` | `tp` | tcpProxies | `role` |
| `kafka_proxies` | `kp` | kafkaProxies | `role` |
| `schedulers` | `sch` | schedulers | — |
| `controller_agents` | `ca` | controllerAgents | — |
| `query_trackers` | `qt` | queryTrackers | — |
| `yql_agents` | `yqla` | yqlAgents | — |
| `cypress_proxies` | `cyp` | cypressProxies | — |
| `bundle_controllers` | `bc` | bundleController | — |

Имена подов строятся как `<короткое_имя>-<группа>-<индекс>` (часть `-<группа>-` опускается для группы `default`). Примеры: `ms-0`, `rp-internal-0`.

### Примеры

Логи мастеров за интервал времени с двумя writer'ами:

```bash
yt admin logs k8s ytbench primary_masters \
  -w access -w debug \
  --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z \
  -o case-1
```

Только конкретные поды:

```bash
yt admin logs k8s ytbench primary_masters -w debug \
  --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z \
  -p ms-3 -p ms-4 -o case-1
```

Логи слота job-proxy на exec-ноде:

```bash
yt admin logs k8s ytbench exec_nodes -w debug \
  --from-ts 2026-02-07T14:38:01Z --to-ts 2026-02-07T14:48:01Z \
  --exec-slot-index 17 -p end-6 -o case-2
```

Серверный grep по содержимому:

```bash
yt admin logs k8s ytbench primary_masters -w debug --grep "Transaction aborted" -o case-3
```

Получить список доступных writer'ов компонента можно из его конфигурации в CR. Например, для `primary_masters`:

```bash
kubectl -n ytsaurus-dev get ytsaurus -o json | jq -r '
  .items[0].spec.primaryMasters |
  (.loggers[]? | select(.writerType == "file") | .name),
  (.structuredLoggers[]? | .name)
'
```

### Структура вывода

```
<output_dir>/
  <pod_name>/
    <log_file>
```

Для слотов exec-нод:

```
<output_dir>/
  <pod_name>/
    slot/
      <slot_id>/
        <log_file>
```

{% note info "Примечание" %}

Утилита использует идентификацию файлов по inode, чтобы корректно выгружать логи даже при их ротации между этапом листинга и выгрузкой. При использовании `--grep` сжатые файлы (`.zstd`, `.gz`) автоматически распаковываются, расширение архива убирается из имени выходного файла.

{% endnote %}

---

## Выгрузка и локальная визуализация метрик Prometheus (`metrics`) { #metrics }

Инструменты для работы с метриками: проверка спецификации выгрузки, выгрузка метрик из Prometheus в архив и воспроизведение архива локально через Prometheus + Grafana, которые разворачиваются локально в Docker.

```bash
yt admin metrics <validate|dump|replay> [options]
```

#### Формат `spec.yaml` { #metrics-spec }

Спецификация описывает, какие метрики выгружать. Структура файла:

- `defaults.step` — шаг дискретизации по умолчанию (можно переопределить опцией `--step`).
- `targets` — список таргетов, каждый со своим `type`:
  - `type: metric` — задаёт PromQL-запрос в поле `query`; из запроса извлекаются селекторы.
  - `type: dashboard` — путь до JSON-файла дашборда Grafana в поле `path` (относительно `spec.yaml`); из всех панелей дашборда извлекаются `expr`. Значения переменных дашборда берутся из его `templating` и могут быть переопределены словарём `params`.

Пример `spec.yaml`:

```yaml
defaults:
  step: 30s

targets:
  - type: metric
    query: yt_resource_tracker_total_cpu{service="master"}

  - type: dashboard
    path: master-accounts.json
    params:
      cluster: clusrer-name
      account: sys
      left_medium: default
      right_medium: ssd_blobs
```

В `params` указываются значения переменных дашборда. Если переменная не задана, берётся её значение по умолчанию из дашборда. Сам файл дашборда (поле `path`) можно [получить из образа `monitoring` или собрать самостоятельно](#dashboards).

Селекторы из нескольких таргетов объединяются и дедуплицируются; посмотреть итоговый список можно командой `metrics validate`.

#### Дашборд для таргета `type: dashboard` { #dashboards }

В поле `path` таргета `type: dashboard` указывается JSON-файл дашборда Grafana. Готовые собранные дашборды {{product-name}} хранятся в репозитории — [`yt/admin/dashboards/yt_dashboards/tests_os/canondata`](https://github.com/ytsaurus/ytsaurus/tree/main/yt/admin/dashboards/yt_dashboards/tests_os/canondata). Оттуда нужный файл можно скачать напрямую и указать путь до него в `path`, например для дашборда `master-accounts`:

```bash
curl -O https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/admin/dashboards/yt_dashboards/tests_os/canondata/test_dashboards.test_master_accounts/master-accounts.json
```

### Проверка спецификации метрик (`validate`) { #metrics-validate }

Проверяет спецификацию метрик (`spec.yaml`) и печатает список уникальных селекторов.

```bash
yt admin metrics validate --spec spec.yaml [--target '<json>' ...]
```

| Параметр | Описание |
| --- | --- |
| `--spec <path>` | Путь до `spec.yaml` (обязательный) |
| `--target <json>` | Дополнительный inline-таргет в JSON, например `'{"type":"metric","query":"yt_x"}'` |

### Выгрузка метрик в архив (`dump`) { #metrics-dump }

Выгружает метрики из Prometheus за заданный интервал в zip-архив.

```bash
yt admin metrics dump --spec spec.yaml \
  --from-ts <ISO8601> --to-ts <ISO8601> \
  --prometheus-url <url> [--step <step>] [--output metrics.zip] [--max-series N] [--force]
```

| Параметр | По умолчанию | Описание |
| --- | --- | --- |
| `--spec <path>` | — | Путь до `spec.yaml` (обязательный) |
| `--from-ts <ISO8601>` | — | Начало интервала (обязательный) |
| `--to-ts <ISO8601>` | — | Конец интервала (обязательный) |
| `--prometheus-url <url>` | — | Базовый URL Prometheus (обязательный) |
| `--step <step>` | из spec | Шаг дискретизации |
| `--target <json>` | — | Дополнительный inline-таргет |
| `--output <path>` | `metrics.zip` | Путь до выходного архива |
| `--max-series <N>` | `100000` | Если суммарное число временных рядов для выгрузки превышает это значение, запрашивается подтверждение |
| `--force` | — | Пропустить подтверждение при превышении `--max-series` |

{% note info "Примечание" %}

Если заданы переменные окружения `PROMETHEUS_USER` и `PROMETHEUS_PASSWORD`, для запросов к Prometheus используется Basic Auth с этими значениями. Иначе запросы выполняются без аутентификации.

{% endnote %}

### Локальное воспроизведение метрик (`replay`) { #metrics-replay }

Импортирует метрики из ранее выгруженного архива во временную директорию и поднимает поверх неё локальные контейнеры Prometheus и Grafana для их просмотра.

```bash
yt admin metrics replay <archive.zip> [--prometheus-port N] [--grafana-port N]
```

| Параметр | По умолчанию | Описание |
| --- | --- | --- |
| `archive` | — | Путь до архива метрик (`.zip`) |
| `--prometheus-port <N>` | свободный порт | Порт для Prometheus |
| `--grafana-port <N>` | свободный порт | Порт для Grafana |

#### Требования

- Доступ к docker-демону (контейнеры Prometheus и Grafana запускаются через него).
- Установленный python-пакет `docker` (входит в `ytsaurus-client[admin]`).

### Пример сценария

```bash
# 1. Проверить спецификацию
yt admin metrics validate --spec spec.yaml

# 2. Выгрузить метрики за интервал (по умолчанию в metrics.zip)
yt admin metrics dump --spec spec.yaml \
  --from-ts 2026-06-03T10:00:00Z --to-ts 2026-06-03T11:00:00Z \
  --prometheus-url https://prometheus.example.com

# 3. Воспроизвести локально
yt admin metrics replay metrics.zip
```

---

## Удалить нераспознанные опции из конфига мастера (`remove-master-unrecognized-options`) { #remove-master-unrecognized-options }

Удаляет нераспознанные опции из динамического конфига мастера (`//sys/@config`).

Команда читает `//sys/@master_alerts`, находит алерт `Found unrecognized options in dynamic cluster config` и удаляет из `//sys/@config` все перечисленные в нём опции. После удаления вложенных опций пустые промежуточные узлы (`{}`) также удаляются. Если такого алерта нет, команда ничего не делает.

Чаще всего это требуется [после обновления кластера](../../admin-guide/update-ytsaurus.md#operator): в новой версии {{product-name}} часть полей конфига может выйти из употребления, и тогда они остаются в `//sys/@config` как нераспознанные.

```bash
yt admin remove-master-unrecognized-options [--dry] [--do-not-print-config]
```

| Параметр | По умолчанию | Описание |
| --- | --- | --- |
| `--dry` | `False` | Только вывести нераспознанные опции, не удаляя их |
| `--do-not-print-config` | `False` | Не печатать `//sys/@config` перед удалением (по умолчанию конфиг печатается) |

### Пример

```bash
# Посмотреть, какие опции будут удалены, ничего не меняя
yt admin remove-master-unrecognized-options --dry

# Удалить нераспознанные опции
yt admin remove-master-unrecognized-options
```

---

## Запуск утилит как standalone-скриптов { #standalone-scripts }

Часть утилит дополнительно собрана как самостоятельные программы в каталоге [`yt/yt/scripts/`](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/scripts). Они переиспользуют те же модули `yt.admin.*`, что и `yt admin`, и могут быть вызваны без установки полного CLI — например, в окружениях, где удобнее запускать модуль напрямую.

| Скрипт | Эквивалент `yt admin` | Что делает |
| --- | --- | --- |
| `fetch_cluster_info` | `describe cluster` / `describe table` | Описание кластера и динамических таблиц |
| `fetch_cluster_logs` | `logs k8s` | Выгрузка логов из Kubernetes |
| `fetch_cluster_metrics` | `metrics validate/dump/replay` | Работа с метриками |
| `remove_master_unrecognized_options` | `remove-master-unrecognized-options` | Удаление нераспознанных опций из динамического конфига мастера |
